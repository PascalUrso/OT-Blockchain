/*
 * SPDX-License-Identifier: Apache-2.0
 */

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.reflect.TypeToken;
import com.google.protobuf.ByteString;
import org.hyperledger.fabric.client.Contract;
import org.hyperledger.fabric.client.Gateway;
import org.hyperledger.fabric.client.Hash;
import org.hyperledger.fabric.client.Network;
import org.hyperledger.fabric.protos.common.Block;
import org.hyperledger.fabric.protos.common.BlockMetadataIndex;
import org.hyperledger.fabric.protos.common.ChannelHeader;
import org.hyperledger.fabric.protos.common.Envelope;
import org.hyperledger.fabric.protos.common.HeaderType;
import org.hyperledger.fabric.protos.common.Payload;
import org.hyperledger.fabric.protos.peer.ChaincodeAction;
import org.hyperledger.fabric.protos.peer.ChaincodeActionPayload;
import org.hyperledger.fabric.protos.peer.ChaincodeEvent;
import org.hyperledger.fabric.protos.peer.ProposalResponsePayload;
import org.hyperledger.fabric.protos.peer.Transaction;
import org.hyperledger.fabric.protos.peer.TransactionAction;
import org.hyperledger.fabric.protos.peer.TxValidationCode;

import java.lang.management.ManagementFactory;
import java.lang.management.ThreadMXBean;
import java.io.IOException;
import java.io.BufferedWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Scanner;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Main client application for OT-based collaborative editing on Hyperledger
 * Fabric.
 *
 * <p>
 * Each instance represents one peer in the editing session. The peer submits
 * operations as Fabric transactions and listens for new blocks to stay in sync.
 * Concurrent edits are reconciled locally using Operational Transformation
 * (OT).
 *
 * <p>
 * Two execution modes are supported:
 * <ul>
 * <li><b>Interactive</b> (default): a REPL lets the user stage and submit
 * operations manually.</li>
 * <li><b>Test</b> ({@code --test <config.json>}): operations are dispatched
 * automatically at
 * predetermined logical clock ticks, enabling reproducible multi-client
 * scenarios.</li>
 * </ul>
 */
public final class App {

    // -------------------------------------------------------------------------
    // Constants
    // -------------------------------------------------------------------------

    /** Maximum number of times a transaction is retried on MVCC conflict. */
    private static final int MAX_SUBMIT_RETRIES = 3;

    /** CLI flag that activates automated test mode. */
    private static final String TEST_MODE_ARG = "--test";

    /** CLI flag that replays a recorded operation log against the ledger. */
    private static final String REPLAY_LOG_ARG = "--replay-log";

    /** Parameters of creating snapshots. */
    private static final int SNAPSHOT_TRIGGER_OPS = 100000;
    private static final long SNAPSHOT_TRIGGER_MS = 60_000L;
    private static final int SNAPSHOT_CHUNK_SIZE = 900_000;
    private static final long PENDING_OP_TTL_MS = 60_000L;

    // -------------------------------------------------------------------------
    // Infrastructure
    // -------------------------------------------------------------------------

    private final Gson gson = new GsonBuilder().setPrettyPrinting().create();
    private final Scanner scanner = new Scanner(System.in);
    private final Random random;
    private final Object stateLock = new Object();

    private final Network network;
    private final Contract contract;
    private final TestRunConfig runConfig;

    /** True when running in automated test mode (no user interaction). */
    private final boolean testMode;

    /** True when running realtime experiment mode. */
    private final boolean realtimeMode;

    // -------------------------------------------------------------------------
    // Session state
    // -------------------------------------------------------------------------

    private String clientId;
    private String docId;

    /** Last snapshot of the document state as seen on the committed ledger. */
    private DocumentState chainState;

    /**
     * Authoritative view of the document, rebuilt by replaying all committed
     * operations in ledger order through the server-side OT pipeline (serverRec).
     */
    private String committedView;

    /**
     * Optimistic local view = committedView with all local pending ops applied on
     * top.
     * This is what the user sees in real time.
     */
    private String localView;

    /**
     * Cursor used to fetch only new operations from the ledger (incremental sync).
     */
    // `lastLogCursorKey` removed: no longer used (events-only sync)

    /**
     * Number of operations committed to the ledger so far (monotonically
     * increasing).
     */
    private long lastSyncedVersion;

    /** Last processed block number from chaincode events. */
    private long lastEventBlock;

    /** Last processed transaction index within lastEventBlock. */
    private int lastEventTxIndex = -1;

    /** Last processed chaincode action index within lastEventBlock. */
    private int lastEventActionIndex = -1;

    /**
     * Number of remote (non-own) operations received since the last submitted op.
     * Piggybacked as the {@code ack} field on the next outgoing transaction so the
     * server-side buffer for this client can be trimmed accordingly.
     */
    private long localAck;

    /**
     * Logical clock incremented on every received committed operation (own or
     * remote).
     * Used in test mode to trigger scheduled operations at deterministic points.
     */
    private int localClock;

    // to record the ops and time since last snapshot for creating snapshots
    private long lastSnapshotVersion = 0;
    private long lastSnapshotTimeMs = System.currentTimeMillis();
    private long nextSnapshotIntervalMs = SNAPSHOT_TRIGGER_MS;

    // -------------------------------------------------------------------------
    // Test / replay state
    // -------------------------------------------------------------------------

    /** Logical clock values at which scheduled test operations should be fired. */
    private final List<Integer> testClocks = new ArrayList<>();

    /**
     * Pre-loaded list of operations to dispatch at specific clock ticks in test
     * mode.
     */
    private final List<ScheduledOp> scheduledOps = new ArrayList<>();

    /**
     * Index into {@code scheduledOps} pointing to the next op yet to be dispatched.
     */
    private int nextScheduledOpIndex = 0;

    // -------------------------------------------------------------------------
    // OT state (server-side algorithm, run locally per client)
    // -------------------------------------------------------------------------

    /**
     * Ordered list of all operations committed to the ledger, after OT
     * transformation.
     * Used to initialise the per-client buffers of late-joining peers.
     */
    private final List<Operation> committedHistory = new ArrayList<>();

    /**
     * Per-client "outstanding" buffers: for a given sender, the buffer holds the
     * committed remote ops that arrived <em>after</em> the sender's last known ack.
     * These are the ops that still need to be transformed against any future
     * submission
     * from that sender.
     */
    private final Map<String, List<Operation>> clientBuffers = new HashMap<>();

    /**
     * Set of all client IDs seen so far; used to broadcast incoming ops to all
     * peers.
     */
    private final Map<String, Long> knownClients = new HashMap<>();

    /**
     * Operations staged locally but not yet confirmed on-chain.
     */
    private final List<Operation> localPending = new ArrayList<>();

    /**
     * IDs of operations already submitted to the Fabric network (transaction sent)
     * but whose block confirmation has not been received yet.
     */
    private final Set<String> submittedPendingOpIds = new HashSet<>();

    /** True once the chaincode event listener has started. */
    private volatile boolean eventListenerStarted;

    /** True after the first submitted operation has carried the cursor metadata. */
    private boolean cursorAttachedToLedger;

    /** Buffer for out-of-order ops by client sequence. */
    private final Map<String, Map<Long, PendingOp>> pendingByClient = new HashMap<>();

    /** Optional metrics sink for test runs. */
    private final MetricsRecorder metrics;

    private final CompletableFuture<Void> readySignal = new CompletableFuture<>();
    private volatile boolean ready = false;
    private boolean receiveStarted = false;
    private long readyNum;
    long[] readyTimeReceive;

    // -------------------------------------------------------------------------
    // Inner types
    // -------------------------------------------------------------------------

    /**
     * A single entry in the test schedule: fire an operation of the given type at
     * the given logical {@code clock} tick. {@code type}, {@code position}, and
     * {@code value} may be null, in which case a random operation is generated.
     */
    private static final class ScheduledOp {
        private final int clock;
        private final String type;
        private final Integer position;
        private final String value;

        private ScheduledOp(final int clock, final String type, final Integer position, final String value) {
            this.clock = clock;
            this.type = type;
            this.position = position;
            this.value = value;
        }
    }

    private static final class PendingOp {
        private final Operation op;
        private final long enqueueTimeMs;

        private PendingOp(final Operation op) {
            this.op = op;
            this.enqueueTimeMs = System.currentTimeMillis();
        }
    }

    /** Simple CSV metrics recorder used in test mode. */
    private static final class MetricsRecorder {
        private final BufferedWriter writer;
        private final ThreadMXBean threadBean;
        private final boolean enabled;

        private MetricsRecorder() {
            this.writer = null;
            this.threadBean = null;
            this.enabled = false;
        }

        private MetricsRecorder(final BufferedWriter writer, final ThreadMXBean threadBean) {
            this.writer = writer;
            this.threadBean = threadBean;
            this.enabled = true;
        }

        static MetricsRecorder fromConfig(final TestRunConfig cfg) {
            if (cfg == null || !cfg.metricsEnabled || cfg.metricsOutputPath == null || cfg.metricsOutputPath.isBlank()) {
                return new MetricsRecorder();
            }
            try {
                Path p = Path.of(cfg.metricsOutputPath).toAbsolutePath().normalize();
                if (p.getParent() != null) {
                    Files.createDirectories(p.getParent());
                }
                boolean newFile = !Files.exists(p);
                BufferedWriter w = Files.newBufferedWriter(p, StandardCharsets.UTF_8,
                        java.nio.file.StandardOpenOption.CREATE, java.nio.file.StandardOpenOption.APPEND);
                if (newFile) {
                    w.write("timestamp,event,type,details,wall_ns,cpu_ns\n");
                    w.flush();
                }
                return new MetricsRecorder(w, ManagementFactory.getThreadMXBean());
            } catch (Exception e) {
                System.out.println("Metrics init failed: " + e.getMessage());
                return new MetricsRecorder();
            }
        }

        long[] start() {
            if (!enabled) {
                return new long[] { 0L, 0L };
            }
            long wall = System.nanoTime();
            long cpu = threadBean != null && threadBean.isCurrentThreadCpuTimeSupported() ? threadBean.getCurrentThreadCpuTime() : 0L;
            return new long[] { wall, cpu };
        }

        void end(final String event, final String type, final String details, final long[] start) {
            if (!enabled) {
                return;
            }
            try {
                long wall = System.nanoTime() - start[0];
                long cpu = threadBean != null && threadBean.isCurrentThreadCpuTimeSupported() ?
                        threadBean.getCurrentThreadCpuTime() - start[1] : 0L;
                String line = System.currentTimeMillis() + "," + event + "," + type + "," + details + "," + wall + "," + cpu + "\n";
                writer.write(line);
                writer.flush();
            } catch (Exception e) {
                System.out.println("Metrics write failed: " + e.getMessage());
            }
        }

        void flush() {
            if (!enabled) {
                return;
            }
            try {
                writer.flush();
                writer.close();
            } catch (Exception ignore) {
            }
        }
    }

    // -------------------------------------------------------------------------
    // Entry point
    // -------------------------------------------------------------------------

    public static void main(final String[] args) throws Exception {
        Logger.getLogger("io.grpc").setLevel(Level.WARNING);
        Logger.getLogger("io.grpc.internal").setLevel(Level.WARNING);

        TestRunConfig config = loadRunConfig(args);
        var grpcChannel = Connections.newGrpcConnection(config.toGatewayProfile());
        var builder = Gateway.newInstance()
                .identity(Connections.newIdentity(config.toGatewayProfile()))
                .signer(Connections.newSigner(config.toGatewayProfile()))
                .hash(Hash.SHA256)
                .connection(grpcChannel)
                .evaluateOptions(options -> options.withDeadlineAfter(5, TimeUnit.SECONDS))
                .endorseOptions(options -> options.withDeadlineAfter(15, TimeUnit.SECONDS))
                .submitOptions(options -> options.withDeadlineAfter(5, TimeUnit.SECONDS))
                .commitStatusOptions(options -> options.withDeadlineAfter(1, TimeUnit.MINUTES));

        try (var gateway = builder.connect()) {
            new App(gateway, config).run();
        } finally {
            grpcChannel.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
        }
    }

    public App(final Gateway gateway, final TestRunConfig runConfig) {
        this.runConfig = runConfig;
        this.testMode = runConfig != null && runConfig.isTestMode();
        this.realtimeMode = runConfig != null && runConfig.isRealtimeMode();
        this.random = createRandom(runConfig);
        this.network = gateway.getNetwork(runConfig.channelName);
        this.contract = network.getContract(runConfig.chaincodeName);
        this.metrics = MetricsRecorder.fromConfig(runConfig);
        this.nextSnapshotIntervalMs = sampleSnapshotIntervalMs();
    }

    private Random createRandom(final TestRunConfig cfg) {
        if (cfg != null && cfg.opTimingSeed != 0L) {
            return new Random(cfg.opTimingSeed);
        }
        return new Random();
    }

    // -------------------------------------------------------------------------
    // Top-level flow
    // -------------------------------------------------------------------------

    private void run() throws Exception {
        if (testMode || realtimeMode) {
            // In test mode all parameters come from the config file.
            clientId = normalize(runConfig.clientId, "userA");
            docId = normalize(runConfig.docId, "doc_1");
            if (testMode) {
                testClocks.clear();
                scheduledOps.clear();
                nextScheduledOpIndex = 0;
                if (runConfig.clocks != null) {
                    testClocks.addAll(runConfig.clocks);
                }
                loadScheduledOps();
                System.out
                        .println("Running in test mode, client=" + clientId + ", doc=" + docId + ", clocks=" + testClocks);
            } else {
                System.out.println("Running in realtime mode, client=" + clientId + ", doc=" + docId
                        + ", durationMs=" + effectiveRealtimeDurationMs());
            }
        } else {
            System.out.print("Enter client_id (default userA): ");
            if (!scanner.hasNextLine()) {
                return;
            }
            clientId = normalize(scanner.nextLine(), "userA");

            System.out.print("Enter doc_id (default doc_1): ");
            if (!scanner.hasNextLine()) {
                return;
            }
            docId = normalize(scanner.nextLine(), "doc_1");
        }


        applyStartupDelay();
        long[] _m_start = metrics.start();
        try {
            bootstrap();
            if (!eventListenerStarted) {
                startBlockListener();
                
            }
        } finally {
            metrics.end("startup", "bootstrap", clientId == null ? "" : clientId, _m_start);
        }
        try {
            readySignal.get(); 
        } catch (ExecutionException e) {
            Thread.currentThread().interrupt();
        }
        resetSnapshotTimer(System.currentTimeMillis());
        localAck = getClientBuffer(clientId).size();

        if (testMode) {
            runTestMode();
            return;
        }        

        if (realtimeMode) {
            runRealtimeMode();
            return;
        }
        
        // Interactive REPL
        while (true) {
            printStatus();
            System.out.println("\n1) INSERT(local pending)  2) DELETE(local pending)  3) UPDATE(local pending)");
            System.out.println(
                    "4) submit 1 local pending  5) submit all local pending  6) manually sync with chain and save snapshot");
            System.out.println("7) view all committed ops  8) exit");
            System.out.print("select: ");
            if (!scanner.hasNextLine()) {
                return;
            }
            String cmd = scanner.nextLine().trim();
            switch (cmd) {
                case "1":
                    stageLocalPending(OperationType.insert);
                    break;
                case "2":
                    stageLocalPending(OperationType.delete);
                    break;
                case "3":
                    stageLocalPending(OperationType.update);
                    break;
                case "4":
                    submitNextPending();
                    break;
                case "5":
                    submitAllPending();
                    break;
                case "6":
                    manualEventSync();
                    try {
                        saveSnapshotToChain();
                        resetSnapshotTimer(System.currentTimeMillis());
                    } catch (Exception e) {
                        System.out.println("Snapshot save failed: " + e.getMessage());
                    }
                    break;
                case "7":
                    List<OperationRecord> allOps = queryAllOps();
                    System.out.println("All committed ops from ledger:");
                    for (OperationRecord record : allOps) {
                        System.out.println(gson.toJson(record));
                    }
                    break;
                case "8":
                    return;
                default:
                    System.out.println("Invalid input");
            }
        }
    }

    /**
     * Automated test mode: polls until all expected operations have been committed,
     * then prints the final state.
     */
    private void runTestMode() throws Exception {
        long timeoutSeconds = runConfig.testTimeoutSeconds <= 0 ? 180 : runConfig.testTimeoutSeconds;
        long pollMillis = runConfig.pollMillis <= 0 ? 300 : runConfig.pollMillis;

        System.out.println("Test mode started, scheduledOps=" + scheduledOps.size()
                + ", timeout=" + timeoutSeconds + "s, poll=" + pollMillis + "ms");

        int totalOps = runConfig.totalOps;
        long deadline = System.currentTimeMillis() + timeoutSeconds * 1000;
        submitScheduledOpsAtClock(localClock);
        // Wait until the ledger has committed all expected operations (or timeout).
        while (lastSyncedVersion < totalOps) {
            if (System.currentTimeMillis() > deadline) {
                System.out.println(
                        "Test mode timed out waiting for " + totalOps + " ops (got " + lastSyncedVersion + ")");
                break;
            }
            TimeUnit.MILLISECONDS.sleep(pollMillis);
        }

        printStatus();
        System.out.println("Test mode finished. dispatched=" + nextScheduledOpIndex + "/" + scheduledOps.size());
    }

    /**
     * Realtime experiment mode: submit random operations continuously for a
     * fixed wall-clock duration, using the configured submission timing mode.
     */
    private void runRealtimeMode() throws Exception {
        long durationMs = effectiveRealtimeDurationMs();
        long pollMillis = runConfig.realtimePollMillis <= 0 ? 100 : runConfig.realtimePollMillis;
        long deadline = System.currentTimeMillis() + durationMs - runConfig.startupDelayMs;
        long nextSubmitAt = System.currentTimeMillis();
        int submitted = 0;

        System.out.println("Realtime mode started, duration=" + durationMs + "ms, opMode="
                + normalize(runConfig.opTimingMode, "poisson") + ", poll=" + pollMillis + "ms");
        
        while (System.currentTimeMillis() < deadline) {
            
            long now = System.currentTimeMillis();
            if (now >= nextSubmitAt) {
                Operation op = getRandomOp();
                stagePendingAndSubmit(localClock, op.getType(), null, null);
                submitted++;
                nextSubmitAt = now + sampleRealtimeOpIntervalMs();
                continue;
            }
            long sleepMs = Math.min(pollMillis, Math.max(1L, nextSubmitAt - now));
            TimeUnit.MILLISECONDS.sleep(sleepMs);
        }

        long settleDeadline = System.currentTimeMillis() + Math.max(0L, runConfig.realtimeDrainTimeoutMs);
        while (hasUnsubmittedPending() && System.currentTimeMillis() < settleDeadline) {
            TimeUnit.MILLISECONDS.sleep(pollMillis);
        }

        printStatus();
        System.out.println("Realtime mode finished. submitted=" + submitted + ", pending=" + localPending.size()
                + ", unsubmitted=" + hasUnsubmittedPending());
    }

    /**
     * Converts the flat clock/type/position/value arrays from the config into a
     * sorted
     * list of {@link ScheduledOp} entries, ready to be dispatched by
     * {@link #submitScheduledOpsAtClock}.
     */
    private void loadScheduledOps() {
        if (testClocks.isEmpty()) {
            return;
        }

        List<String> cfgTypes = runConfig.types == null ? new ArrayList<>() : runConfig.types;
        List<Integer> cfgPositions = runConfig.positions == null ? new ArrayList<>() : runConfig.positions;
        List<String> cfgValues = runConfig.values == null ? new ArrayList<>() : runConfig.values;

        // "Detailed" mode: each clock tick has an explicit type/position/value.
        boolean hasDetailed = !cfgTypes.isEmpty() && cfgTypes.size() == testClocks.size();

        for (int i = 0; i < testClocks.size(); i++) {
            int clock = testClocks.get(i);
            if (hasDetailed) {
                String type = cfgTypes.get(i);
                Integer position = i < cfgPositions.size() ? cfgPositions.get(i) : null;
                String value = i < cfgValues.size() ? cfgValues.get(i) : "";
                scheduledOps.add(new ScheduledOp(clock, type, position, value));
            } else {
                // Random op will be generated at dispatch time.
                scheduledOps.add(new ScheduledOp(clock, null, null, null));
            }
        }

        scheduledOps.sort((a, b) -> Integer.compare(a.clock, b.clock));
    }

    private void applyStartupDelay() throws InterruptedException {
        long delayMs = runConfig == null ? 0L : Math.max(0L, runConfig.startupDelayMs);
        if (delayMs > 0) {
            System.out.println("Startup delayed by " + delayMs + "ms");
            TimeUnit.MILLISECONDS.sleep(delayMs);
        }
    }

    private long effectiveRealtimeDurationMs() {
        return runConfig != null && runConfig.experimentDurationMs > 0 ? runConfig.experimentDurationMs : 300_000L;
    }

    private long sampleRealtimeOpIntervalMs() {
        String mode = normalize(runConfig.opTimingMode, "poisson").toLowerCase();
        long meanMs = runConfig.opTimingMeanMs > 0 ? runConfig.opTimingMeanMs : 1_000L;
        long windowMs = Math.max(0L, runConfig.opTimingWindowMs);
        int burstSize = runConfig.opBurstSize > 0 ? runConfig.opBurstSize : 1;
        long burstGapMs = Math.max(0L, runConfig.opBurstGapMs);
        long burstJitterMs = Math.max(0L, runConfig.opBurstJitterMs);
        long min = Math.max(1L, Math.round(meanMs * 0.9d));
        long max = Math.max(min, Math.round(meanMs * 1.1d));

        long sampled;
        switch (mode) {
            case "simultaneous":
                sampled = 0L;
                break;
            case "uniform":
                sampled = random.nextInt((int) Math.min(Integer.MAX_VALUE, Math.max(1L, windowMs)) + 1);
                break;
            case "burst":
                sampled = burstGapMs + (burstJitterMs > 0 ? random.nextInt((int) Math.min(Integer.MAX_VALUE, burstJitterMs) + 1) : 0);
                break;
            case "poisson":
            default:
                sampled = Math.round(random.nextGaussian() * (meanMs * 0.1d) + meanMs);
                break;
        }

        return Math.max(min, Math.min(max, sampled));
    }

    // -------------------------------------------------------------------------
    // Initialisation
    // -------------------------------------------------------------------------

    /**
     * Initialises local state and bootstraps the document on-chain.
     * If the document already exists (another client got there first) the
     * InitDoc transaction will throw — that exception is silently ignored.
     */
        private void bootstrap() throws Exception {
        try {
            contract.evaluateTransaction("QueryState", docId);
        } catch (Exception e) {
            // Document was not created by another client, create it.
            try{
                contract.submitTransaction("InitDoc", docId, "");
            } catch (Exception e2) {
                // Ignore exception, as the document might have been created by another client.
            }
        }

        long[] readyTime = metrics.start();
        readyNum = queryTotalNumOpsFromWorldState();
        System.out.println("Total number of ops in world state: " + readyNum);

        localAck = 0;
        localClock = 0;
        cursorAttachedToLedger = false;
        localPending.clear();
        submittedPendingOpIds.clear();
        

        if (!loadSnapshotFromChain()) {
            committedView = "";
            chainState = new DocumentState(docId, committedView, 0);
            lastSyncedVersion = chainState.getVersion();
            localView = committedView;
            knownClients.clear();
            knownClients.put(clientId, 0L);
            clientBuffers.clear();
            clientBuffers.put(clientId, new ArrayList<>());
            lastEventBlock = 0;
            localPending.clear();
            submittedPendingOpIds.clear();
            lastSyncedVersion = 0;
            lastEventTxIndex = -1;
            lastEventActionIndex = -1;
        }
        if(lastSyncedVersion >= readyNum){
            ready = true;
            readySignal.complete(null);
        }
        metrics.end("startup", "getreadyPart1", String.valueOf(lastSyncedVersion), readyTime);
        
        System.out.println(
                "Initialized: version=" + chainState.getVersion() + ", content='" + chainState.getContent() + "'");
    }
    /**
     * loadSnapshotFromChain
     * Called once on startup.
     */
    private boolean loadSnapshotFromChain() {
        long[] _m_start = metrics.start();
        try {
            DocumentSnapshot snapshot = queryLatestSnapshot();
            if (snapshot == null || snapshot.committedView == null) {
                return false;
            }

            committedView = snapshot.committedView;
            localView = committedView;
            lastSyncedVersion = snapshot.version;
            chainState = new DocumentState(docId, committedView, lastSyncedVersion);
            lastSnapshotVersion = snapshot.version;
            lastSnapshotTimeMs = System.currentTimeMillis();
            nextSnapshotIntervalMs = sampleSnapshotIntervalMs();
            lastEventBlock = snapshot.lastBlockNumber;
            lastEventTxIndex = snapshot.lastEventTxIndex;
            lastEventActionIndex = snapshot.lastEventActionIndex;
            committedHistory.clear();
            if (snapshot.committedHistory != null) {
                committedHistory.addAll(snapshot.committedHistory);
            }
            knownClients.clear();
            if (snapshot.knownClients != null) {
                knownClients.putAll(snapshot.knownClients);
            }
            knownClients.putIfAbsent(clientId, 0L);

            clientBuffers.clear();
            if (snapshot.clientBuffers != null) {
                clientBuffers.putAll(snapshot.clientBuffers);
            }
            clientBuffers.putIfAbsent(clientId, new ArrayList<>(committedHistory));

            pendingByClient.clear();
            if (snapshot.pendingByClient != null) {
                for (Map.Entry<String, Map<Long, Operation>> entry : snapshot.pendingByClient.entrySet()) {
                    if (entry.getValue() == null || entry.getValue().isEmpty()) {
                        continue;
                    }
                    Map<Long, PendingOp> pending = new HashMap<>();
                    for (Map.Entry<Long, Operation> opEntry : entry.getValue().entrySet()) {
                        if (opEntry.getValue() != null) {
                            pending.put(opEntry.getKey(), new PendingOp(opEntry.getValue()));
                        }
                    }
                    if (!pending.isEmpty()) {
                        pendingByClient.put(entry.getKey(), pending);
                    }
                }
            }
            localView = committedView;
            metrics.end("snapshot", "load", "version=" + snapshot.version, _m_start);
            return true;
        } catch (Exception e) {
            System.out.println("Snapshot load failed: " + e.getMessage());
            metrics.end("snapshot", "load", "error", _m_start);
            return false;
        }
    }

    private DocumentSnapshot queryLatestSnapshot() throws Exception {
        byte[] result = contract.evaluateTransaction("GetLatestSnapshot", docId);
        if (result == null || result.length == 0) {
            return null;
        }
        String json = new String(result, StandardCharsets.UTF_8);
        if (json.isBlank()) {
            return null;
        }
        return gson.fromJson(json, DocumentSnapshot.class);
    }

    // -------------------------------------------------------------------------
    // Block listener
    // -------------------------------------------------------------------------

    /**
     * Starts a background daemon thread that subscribes to Fabric block events.
     * On each new block the client re-syncs its local state from the ledger.
     */
    private void startBlockListener() {
        if (eventListenerStarted) {
            return;
        }
        eventListenerStarted = true;
        Thread t = new Thread(() -> {
            try (var blocks = network
                    .newBlockEventsRequest()
                    .startBlock(lastEventBlock)
                    .build()
                    .getEvents()) {
                blocks.forEachRemaining(block -> {
                    try {
                        processBlock(block);
                    } catch (Exception e) {
                        System.out.println("Block handling failed: " + e.getMessage());
                    }
                });
            } catch (Exception e) {
                System.out.println("Chaincode event listener stopped: " + e.getMessage());
            } finally {
                eventListenerStarted = false;
            }
        });
        t.setDaemon(true);
        t.start();
    }

    private void processBlock(final Block block) {
        if(!receiveStarted && !ready){
            receiveStarted = true;
            readyTimeReceive = metrics.start();
        }
        if (block == null) {
            return;
        }
        long blockNumber = block.getHeader().getNumber();
        List<ByteString> txs = block.getData().getDataList();
        ByteString txFilter = ByteString.EMPTY;
        if (block.hasMetadata()
                && block.getMetadata().getMetadataCount() > BlockMetadataIndex.TRANSACTIONS_FILTER_VALUE) {
            txFilter = block.getMetadata().getMetadataList().get(BlockMetadataIndex.TRANSACTIONS_FILTER_VALUE);
        }

        for (int txIndex = 0; txIndex < txs.size(); txIndex++) {
            if (blockNumber < lastEventBlock) {
                continue;
            }
            if (blockNumber == lastEventBlock && txIndex < lastEventTxIndex) {
                continue;
            }

            if (!txFilter.isEmpty() && txIndex < txFilter.size()) {
                int validationCode = txFilter.byteAt(txIndex);
                if (TxValidationCode.forNumber(validationCode) != TxValidationCode.VALID) {
                    continue;
                }
            }

            try {
                Envelope envelope = Envelope.parseFrom(txs.get(txIndex));
                Payload payload = Payload.parseFrom(envelope.getPayload());
                if (!payload.hasHeader()) {
                    continue;
                }

                ChannelHeader channelHeader = ChannelHeader.parseFrom(payload.getHeader().getChannelHeader());
                if (channelHeader.getType() != HeaderType.ENDORSER_TRANSACTION_VALUE) {
                    continue;
                }

                Transaction tx = Transaction.parseFrom(payload.getData());
                List<TransactionAction> actions = tx.getActionsList();
                for (int actionIndex = 0; actionIndex < actions.size(); actionIndex++) {
                    if (blockNumber == lastEventBlock
                            && txIndex == lastEventTxIndex
                            && actionIndex <= lastEventActionIndex) {
                        continue;
                    }

                    TransactionAction action = actions.get(actionIndex);
                    ChaincodeActionPayload actionPayload = ChaincodeActionPayload.parseFrom(action.getPayload());
                    if (!actionPayload.hasAction()) {
                        continue;
                    }

                    ProposalResponsePayload responsePayload = ProposalResponsePayload.parseFrom(
                            actionPayload.getAction().getProposalResponsePayload());
                    ChaincodeAction chaincodeAction = ChaincodeAction.parseFrom(responsePayload.getExtension());
                    if (chaincodeAction.getEvents().isEmpty()) {
                        continue;
                    }

                    ChaincodeEvent event = ChaincodeEvent.parseFrom(chaincodeAction.getEvents());
                    if (!runConfig.chaincodeName.equals(event.getChaincodeId())) {
                        continue;
                    }
                    if (!("SubmitOp::" + docId).equals(event.getEventName())) {
                        continue;
                    }

                    byte[] payloadBytes = event.getPayload().toByteArray();
                    if (payloadBytes.length == 0) {
                        continue;
                    }

                    OperationRecord record = gson.fromJson(
                            new String(payloadBytes, StandardCharsets.UTF_8),
                            OperationRecord.class);
                    if (record != null && record.getDocId() != null && !record.getDocId().equals(docId)) {
                        continue;
                    }
                    applyCommittedRecordFromEvent(record, blockNumber, txIndex, actionIndex);
                    tryCreateSnapshot();
                }
            } catch (Exception e) {
                System.out.println("Block parse failed: " + e.getMessage());
            }
        }
    }

    private void manualEventSync() {
        if (!eventListenerStarted) {
            System.out.println("Event listener not running; starting it now.");
            startBlockListener();
        }
        System.out.println("Event-based sync active. Last block=" + lastEventBlock
                + ", version=" + lastSyncedVersion);
    }

    // -------------------------------------------------------------------------
    // Local operation staging
    // -------------------------------------------------------------------------

    /**
     * Creates an operation from user input, applies it optimistically to the local
     * view, and appends it to {@code localPending} (not yet submitted to the
     * chain).
     */
    private void stageLocalPending(final OperationType type) {
        int pos = readInt("position: ");
        String value = "";
        if (type == OperationType.insert || type == OperationType.update) {
            System.out.print("value: ");
            value = scanner.nextLine();
        }

        try {
            Operation op = new Operation(
                    UUID.randomUUID().toString(),
                    clientId,
                    type,
                    pos,
                    value,
                    Instant.now().toEpochMilli(),
                    0,
                    -1L,
                    -1L,
                    -1,
                    -1);
            synchronized (stateLock) {
                localView = OTEngine.apply(localView, op);
                localPending.add(op);
            }
            System.out.println("Staged locally: type=" + op.getType() + ", pos=" + op.getPosition()
                    + ", value='" + op.getValue() + "'");
        } catch (Exception e) {
            System.out.println("Failed to apply operation locally: " + e.getMessage());
        }
    }

    // -------------------------------------------------------------------------
    // Submission
    // -------------------------------------------------------------------------

    /**
     * Submits the next unconfirmed pending operation to the Fabric network.
     * Retries up to {@link #MAX_SUBMIT_RETRIES} times on MVCC conflict, re-syncing
     * from the ledger between each attempt so the ack value stays accurate.
     */
    private void submitNextPending() {
        if (!hasUnsubmittedPending()) {
            System.out.println("No unsubmitted local pending operations");
            return;
        }
        Exception lastError = null;

        for (int attempt = 1; attempt <= MAX_SUBMIT_RETRIES; attempt++) {
            Operation candidate;
            long ackSnapshot;
            boolean includeCursor;

            long seq;
            try {
                seq = queryClientSeqFromWorldState();
            } catch (Exception e) {
                System.out.println("Failed to read client seq: " + e.getMessage());
                return;
            }
            Operation candidateForSubmit;
            synchronized (stateLock) {
                candidate = getNextUnsubmittedPending();
                if (candidate == null) {
                    System.out.println("No unsubmitted local pending operations");
                    return;
                }
                ackSnapshot = localAck;
                includeCursor = !cursorAttachedToLedger;

                candidateForSubmit = withAckAndSeq(candidate, ackSnapshot, seq, includeCursor);
                long[] _m_start = metrics.start();
                submittedPendingOpIds.add(candidateForSubmit.getOpId());
                // Reset ack after a successful submission.
                localAck = 0;
                cursorAttachedToLedger = true;
                metrics.end("op", "submit", candidateForSubmit.getOpId(), _m_start);
            }
            try {
                long[] _m_start = metrics.start();
                contract.submitTransaction("SubmitOp", docId, gson.toJson(candidateForSubmit));
                metrics.end("op", "submitcommunication", candidateForSubmit.getOpId(), _m_start);

                System.out.println(
                        "Tx successfully sent, op_id=" + candidateForSubmit.getOpId() + ", ack=" + ackSnapshot);
                return;
            } catch (Exception e) {
                submittedPendingOpIds.remove(candidateForSubmit.getOpId());
                localAck += ackSnapshot;
                if (isMvccConflict(e) && attempt < MAX_SUBMIT_RETRIES) {
                    System.out.println("MVCC conflict detected, syncing and retrying (" + attempt + "/"
                            + MAX_SUBMIT_RETRIES + ")");
                    manualEventSync();
                    lastError = e;
                }
            }
            
        }
        if (lastError != null) {
            System.out.println("Submit failed: " + lastError.getMessage());
        } else {
            System.out.println("Submit succeded.");
        }
        return;
    }

    /**
     * Submits all unconfirmed pending operations one by one, stopping if a
     * submission does not advance the queue (e.g. repeated failure).
     */
    private void submitAllPending() {
        if (!hasUnsubmittedPending()) {
            System.out.println("No unsubmitted local pending operations");
            return;
        }

        int submitted = 0;
        while (hasUnsubmittedPending()) {
            Operation before = getNextUnsubmittedPending();
            submitNextPending();
            Operation after = getNextUnsubmittedPending();
            if (before != null && (after == null || !before.getOpId().equals(after.getOpId()))) {
                submitted++;
            } else {
                break;
            }
        }
        System.out.println("Batch submit done, sent " + submitted + " transaction(s) (awaiting block confirmation)");
    }

    private boolean hasUnsubmittedPending() {
        return localPending.stream().anyMatch(op -> !submittedPendingOpIds.contains(op.getOpId()));
    }

    private Operation getNextUnsubmittedPending() {
        for (Operation op : localPending) {
            if (!submittedPendingOpIds.contains(op.getOpId())) {
                return op;
            }
        }
        return null;
    }

    /**
     * Walks the exception chain looking for Fabric MVCC/phantom-read conflict
     * markers.
     */
    private boolean isMvccConflict(final Exception exception) {
        Throwable current = exception;
        while (current != null) {
            String msg = current.getMessage();
            if (msg != null && (msg.contains("MVCC_READ_CONFLICT") || msg.contains("PHANTOM_READ_CONFLICT"))) {
                return true;
            }
            current = current.getCause();
        }
        return false;
    }

    // -------------------------------------------------------------------------
    // Test-mode helpers
    // -------------------------------------------------------------------------

    /**
     * Generates a random valid operation against the current {@code localView}.
     * Ensures insert is used when the document is empty (delete/update would be
     * invalid).
     */
    private Operation getRandomOp() {
        int len = localView == null ? 0 : localView.length();
        OperationType randomType;
        if (len == 0) {
            randomType = OperationType.insert;
        } else {
            OperationType[] types = OperationType.values();
            randomType = types[random.nextInt(types.length)];
        }

        int randomPos;
        String randomValue = "";
        if (randomType == OperationType.insert) {
            randomPos = random.nextInt(len + 1);
            randomValue = String.valueOf((char) ('a' + random.nextInt(26)));
        } else if (randomType == OperationType.delete) {
            randomPos = random.nextInt(len);
        } else {
            randomPos = random.nextInt(len);
            randomValue = String.valueOf((char) ('a' + random.nextInt(26)));
        }

        return new Operation(
                UUID.randomUUID().toString(),
                clientId,
                randomType,
                randomPos,
                randomValue,
                Instant.now().toEpochMilli(),
                0,
                -1L,
                -1L,
                -1,
                -1);
    }

    /**
     * Stages an operation (clamping position to valid bounds), applies it to the
     * local view, and immediately submits it to the chain.
     * Used by the test scheduler and the replay engine.
     */
    private void stagePendingAndSubmit(final int clock,
            final OperationType type,
            final Integer requestedPosition,
            final String requestedValue) {
        try {
            Operation op;
            synchronized (stateLock) {
                int len = localView == null ? 0 : localView.length();
                OperationType finalType = type;
                int finalPos;
                String finalValue = requestedValue == null ? "" : requestedValue;

                // Downgrade delete/update to insert when the document is empty.
                if (finalType == OperationType.delete || finalType == OperationType.update) {
                    if (len <= 0) {
                        finalType = OperationType.insert;
                    }
                }

                // Clamp position to the valid range for the chosen operation type.
                if (finalType == OperationType.insert) {
                    int pos = requestedPosition == null ? random.nextInt(len + 1) : requestedPosition;
                    finalPos = Math.max(0, Math.min(pos, len));
                    if (finalValue.isEmpty()) {
                        finalValue = randomLowercaseValue();
                    }
                } else if (finalType == OperationType.delete) {
                    int pos = requestedPosition == null ? random.nextInt(len) : requestedPosition;
                    finalPos = Math.max(0, Math.min(pos, len - 1));
                    finalValue = "";
                } else {
                    int pos = requestedPosition == null ? random.nextInt(len) : requestedPosition;
                    finalPos = Math.max(0, Math.min(pos, len - 1));
                    if (finalValue.isEmpty()) {
                        finalValue = randomLowercaseValue();
                    }
                }

                op = new Operation(
                        UUID.randomUUID().toString(),
                        clientId,
                        finalType,
                        finalPos,
                        finalValue,
                        Instant.now().toEpochMilli(),
                        0,
                        -1L,
                        -1L,
                        -1,
                        -1);

                localView = OTEngine.apply(localView, op);
                localPending.add(op);
            }

            submitNextPending();
            System.out.println("Submitted op at clock=" + clock + ", opId=" + op.getOpId()
                    + ", type=" + op.getType() + ", pos=" + op.getPosition() + ", value='" + op.getValue() + "'");
        } catch (Exception e) {
            System.out.println("Failed scheduled op at clock=" + clock + ": " + e.getMessage());
        }
    }

    private String randomLowercaseValue() {
        return String.valueOf((char) ('a' + random.nextInt(26)));
    }

    // -------------------------------------------------------------------------
    // OT core: clientRec
    // -------------------------------------------------------------------------

    /**
     * Client-side receive: integrates a batch of newly committed operations
     * (already processed by {@link #serverRec}) into the local view.
     *
     * <p>
     * Algorithm for each incoming op {@code o}:
     * <ol>
     * <li>Increment the logical clock.</li>
     * <li>If {@code o} originated from this client: the corresponding pending op
     * has
     * been confirmed — remove it from {@code localPending} and skip re-applying it
     * (it was already reflected in the local view).</li>
     * <li>Otherwise: transform {@code o} against every op in {@code localPending}
     * (and symmetrically transform each pending op against {@code o}), then
     * apply the transformed {@code o} to {@code localView}.
     * Increment {@code localAck} to signal one more remote op acknowledged.</li>
     * <li>After processing each op, fire any scheduled test operations whose clock
     * trigger matches the current clock.</li>
     * </ol>
     */
    private void clientRec(final List<Operation> transformedBlockOps) {
        for (Operation op : transformedBlockOps) {
            localClock++;

            if (!localPending.isEmpty() && op.getClientId().equals(clientId) && op.getOpId().equals(localPending.get(0).getOpId())) {
                // This committed op is our own: the first pending entry is confirmed.
                synchronized (stateLock) {
                    localPending.subList(0, 1).clear();
                    submittedPendingOpIds.remove(op.getOpId());
                }
                submitScheduledOpsAtClock(localClock);
                continue;
            }

            // Remote op: transform it against each local pending op (and vice-versa),
            // keeping localPending consistent with the updated committed history.
            synchronized (stateLock) {
                localAck++;
                for (int i = 0; i < localPending.size(); i++) {
                    Operation pendingOp = localPending.get(i);
                    Operation pendingPrime = OTEngine.transform(pendingOp, op);
                    Operation opPrime = OTEngine.transform(op, pendingOp);
                    op = opPrime;
                    localPending.set(i, pendingPrime);
                }
                localView = OTEngine.apply(localView, op);
            }
            submitScheduledOpsAtClock(localClock);
        }
    }

    /**
     * Fires all scheduled test operations whose logical clock matches
     * {@code clock}.
     * Does nothing in interactive mode.
     */
    private void submitScheduledOpsAtClock(final int clock) {
        if (!testMode || scheduledOps.isEmpty()) {
            return;
        }

        while (nextScheduledOpIndex < scheduledOps.size()) {
            ScheduledOp next = scheduledOps.get(nextScheduledOpIndex);
            if (next.clock != clock) {
                break;
            }

            if (next.type == null || next.type.isBlank()) {
                Operation randomOp = getRandomOp();
                stagePendingAndSubmit(clock, randomOp.getType(), randomOp.getPosition(), randomOp.getValue());
            } else {
                OperationType type;
                try {
                    type = OperationType.valueOf(next.type.trim().toLowerCase());
                } catch (Exception ignore) {
                    type = OperationType.insert;
                }
                stagePendingAndSubmit(clock, type, next.position, next.value);
            }

            nextScheduledOpIndex++;
        }
    }

    // -------------------------------------------------------------------------
    // OT core: syncFromChain + serverRec
    // -------------------------------------------------------------------------

    /**
     * Fetches all operations committed since the last sync, runs them through the
     * server-side OT pipeline, then integrates the result into the local view via
     * {@link #clientRec}.
     *
     * <p>
     * Synchronised to prevent concurrent execution by the block-listener thread
     * and the main thread (e.g. manual sync from the menu).
     */

    private void applyCommittedRecordFromEvent(final OperationRecord record, final long blockNumber,
            final int txIndex, final int actionIndex) {
        long[] _m_start = metrics.start();
        
        lastEventBlock = blockNumber;
        lastEventTxIndex = txIndex;
        lastEventActionIndex = actionIndex;

        if (record == null || record.getOperation() == null) {
            return;
        }

        List<Operation> orderedOps = collectOrderedOps(record.getOperation(), txIndex, actionIndex);
        if (orderedOps.isEmpty()) {
            return;
        }

        chainState = new DocumentState(docId, committedView, lastSyncedVersion);
        clientRec(orderedOps);
        System.out.println("Synced from block=" + blockNumber
            + ", version=" + chainState.getVersion() + ", opId=" + record.getOperation().getOpId());
        if(chainState.getVersion() % 10 == 0) {
            printStatus();
        }
        if(!ready && lastSyncedVersion >= readyNum) {
            ready = true;
            readySignal.complete(null);
            metrics.end("startup", "getreadyPart2", String.valueOf(lastSyncedVersion), readyTimeReceive);
        }
        metrics.end("op", "receive", record != null && record.getOperation() != null ? record.getOperation().getOpId() : "", _m_start);
    }

    /**
     * Server-side receive (dOPT-inspired): integrates one newly committed operation
     * into the local copy of the authoritative document state.
     *
     * <p>
     * Algorithm:
     * <ol>
     * <li>Acknowledge previously seen ops by trimming the sender's buffer by
     * {@code incoming.ack} entries.</li>
     * <li>Transform {@code incoming} against every op remaining in the sender's
     * buffer (ops the sender had not yet seen when it produced this op),
     * and symmetrically update those buffer entries.</li>
     * <li>Apply the fully-transformed op to {@code committedView}.</li>
     * <li>Distribute the transformed op to all other known clients' buffers so
     * their future submissions can be transformed correctly.</li>
     * </ol>
     *
     * @return the fully-transformed version of {@code incomingRaw}
     */
    private Operation serverRecOne(final Operation incomingRaw) {
        String senderId = incomingRaw.getClientId();
        if (senderId == null || senderId.isEmpty()) {
            senderId = "unknown";
        }

        // Trim the sender's buffer: the ack value tells us how many remote ops
        // the sender had already seen when it produced this operation.
        List<Operation> senderBuffer = getClientBuffer(senderId);
        int ack = normalizeAck(incomingRaw.getAck(), senderBuffer.size());
        if (ack > 0) {
            senderBuffer.subList(0, ack).clear();
        }
        // Transform incoming against the remaining buffer ops (ops the sender missed).
        Operation incoming = incomingRaw;
        for (int i = 0; i < senderBuffer.size(); i++) {
            Operation bufferOp = senderBuffer.get(i);
            Operation bufferPrime = OTEngine.transform(bufferOp, incoming);
            Operation incomingPrime = OTEngine.transform(incoming, bufferOp);
            senderBuffer.set(i, bufferPrime);
            incoming = incomingPrime;
        }

        committedView = OTEngine.apply(committedView, incoming);
        lastSyncedVersion++;
        committedHistory.add(incoming);

        // Broadcast the transformed op to every other client's buffer.
        List<String> recipients = new ArrayList<>(knownClients.keySet());
        for (String receiverId : recipients) {
            if (receiverId == null || receiverId.equals(senderId)) {
                continue;
            }
            getClientBuffer(receiverId).add(incoming);
        }

        return incoming;
    }

    // -------------------------------------------------------------------------
    // Save snapshot to chain
    // -------------------------------------------------------------------------
    private void tryCreateSnapshot() {
        if (runConfig != null && !runConfig.snapshotEnabled) {
            return;
        }

        long now = System.currentTimeMillis();
        long opsSinceSnapshot = lastSyncedVersion - lastSnapshotVersion;
        boolean opsTrigger = opsSinceSnapshot >= SNAPSHOT_TRIGGER_OPS;
        boolean timeTrigger = (now - lastSnapshotTimeMs) >= nextSnapshotIntervalMs;
        if ((opsTrigger || timeTrigger) && lastSyncedVersion > lastSnapshotVersion) {
            try {
                saveSnapshotToChain();
                lastSnapshotVersion = lastSyncedVersion;
                lastSnapshotTimeMs = now;
                nextSnapshotIntervalMs = sampleSnapshotIntervalMs();
            } catch (Exception e) {
                System.out.println("Snapshot save failed: " + e.getMessage());
            }
        }
    }

    private void saveSnapshotToChain() throws Exception {
        if (runConfig != null && !runConfig.snapshotEnabled) {
            System.out.println("Snapshot persistence disabled by config.");
            return;
        }
        long[] _m_start = metrics.start();
        DocumentSnapshot snapshot = null;
        try {
            snapshot = new DocumentSnapshot();
            snapshot.snapshotId = UUID.randomUUID().toString();
            snapshot.docId = docId;
            snapshot.version = lastSyncedVersion;
            snapshot.timestamp = System.currentTimeMillis();
            snapshot.lastBlockNumber = lastEventBlock;
            snapshot.lastEventTxIndex = lastEventTxIndex;
            snapshot.lastEventActionIndex = lastEventActionIndex;
            snapshot.committedView = committedView;

            snapshot.clientBuffers = new HashMap<>(clientBuffers);
            snapshot.pendingByClient = new HashMap<>();
            for (Map.Entry<String, Map<Long, PendingOp>> entry : pendingByClient.entrySet()) {
                if (entry.getValue() == null || entry.getValue().isEmpty()) {
                    continue;
                }
                Map<Long, Operation> ops = new HashMap<>();
                for (Map.Entry<Long, PendingOp> pendingEntry : entry.getValue().entrySet()) {
                    PendingOp pending = pendingEntry.getValue();
                    if (pending != null && pending.op != null) {
                        ops.put(pendingEntry.getKey(), pending.op);
                    }
                }
                if (!ops.isEmpty()) {
                    snapshot.pendingByClient.put(entry.getKey(), ops);
                }
            }
            snapshot.knownClients = new HashMap<>(knownClients);
            snapshot.committedHistory = new ArrayList<>(committedHistory);

            String snapshotJson = gson.toJson(snapshot);
            List<String> chunks = splitIntoChunks(snapshotJson, SNAPSHOT_CHUNK_SIZE);
            for (int i = 0; i < chunks.size(); i++) {
                contract.submitTransaction("SaveSnapshotChunk",
                        docId,
                        snapshot.snapshotId,
                        String.valueOf(i),
                        String.valueOf(chunks.size()),
                        chunks.get(i));
            }

            SnapshotPointer pointer = new SnapshotPointer(
                    snapshot.docId,
                    snapshot.snapshotId,
                    snapshot.version,
                    snapshot.timestamp,
                    chunks.size());
            contract.submitTransaction("CommitSnapshotPointer", docId, gson.toJson(pointer));
            System.out.println("Snapshot saved: version=" + snapshot.version);
        } finally {
            String details = snapshot == null ? "unknown" : "version=" + snapshot.version;
            metrics.end("snapshot", "save", details, _m_start);
        }
    }

    private void resetSnapshotTimer(final long now) {
        lastSnapshotTimeMs = now;
        nextSnapshotIntervalMs = sampleSnapshotIntervalMs();
    }

    private long sampleSnapshotIntervalMs() {
        long mean = runConfig != null && runConfig.snapshotIntervalMs > 0 ? runConfig.snapshotIntervalMs
                : SNAPSHOT_TRIGGER_MS;
        long min = Math.max(1L, Math.round(mean * 0.8d));
        long max = Math.max(min, Math.round(mean * 1.2d));
        double sigma = Math.max(1.0d, mean * 0.1d);
        long sampled = Math.round(mean + random.nextGaussian() * sigma);
        return Math.max(min, Math.min(max, sampled));
    }

    // -------------------------------------------------------------------------
    // Ledger queries
    // -------------------------------------------------------------------------

    /** Fetches the full operation log for the document. */
    private List<OperationRecord> queryAllOps() throws Exception {
        byte[] result = contract.evaluateTransaction("QueryAllOps", docId);
        return gson.fromJson(new String(result, StandardCharsets.UTF_8), new TypeToken<List<OperationRecord>>() {
        }.getType());
    }

    // -------------------------------------------------------------------------
    // Buffer / ack helpers
    // -------------------------------------------------------------------------

    private List<Operation> getClientBuffer(final String targetClientId) {
        return clientBuffers.computeIfAbsent(targetClientId, id -> buildInitialBufferForClient(id));
    }


    /**
     * Builds an initial buffer for a newly-seen client by reloading ledger ops
     * after the cursor stored in the first submitted op.
     */
    private List<Operation> buildInitialBufferForClient(final String targetClientId) {
        List<Operation> initial = new ArrayList<>();
        // Prefer using in-memory committedHistory if available: reconstruct buffer
        // from committedHistory and the ack carried in referenceOp.
        if (committedHistory != null && !committedHistory.isEmpty()) {
            for (int i = 0; i < committedHistory.size(); i++) {
                Operation committed = committedHistory.get(i);
                if (committed.getClientId() != null && committed.getClientId().equals(targetClientId)) {
                    continue;
                }
                initial.add(committed);
            }
        }
        return initial;
    }


    /**
     * Returns a copy of {@code op} with the given {@code ack} and {@code clientSeq}
     * attached.
     */
    private Operation withAckAndSeq(final Operation op, final long ack, final long clientSeq,
            final boolean includeCursor) {
        if (includeCursor) {
            return new Operation(
                    op.getOpId(),
                    op.getClientId(),
                    op.getType(),
                    op.getPosition(),
                    op.getValue(),
                    op.getTimestamp(),
                    ack,
                    clientSeq,
                    lastEventBlock,
                    lastEventTxIndex,
                    lastEventActionIndex);
        } else {
            return new Operation(
                    op.getOpId(),
                    op.getClientId(),
                    op.getType(),
                    op.getPosition(),
                    op.getValue(),
                    op.getTimestamp(),
                    ack,
                    clientSeq,
                    -1L,
                    -1,
                    -1);
        }
    }

    /**
     * Clamps an ack value to the range [0, bufferSize], guarding against
     * out-of-range or overflow values coming from the network.
     */
    private int normalizeAck(final long ackValue, final int bufferSize) {
        if (ackValue <= 0) {
            return 0;
        }
        if (ackValue > Integer.MAX_VALUE) {
            return bufferSize;
        }
        return Math.min((int) ackValue, bufferSize);
    }

    // -------------------------------------------------------------------------
    // Utilities
    // -------------------------------------------------------------------------

    /* buildLogCursorKey removed: events-only sync, cursor no longer used */

    private void printStatus() {
        System.out.println("\n==== Current Status ====");
        System.out.println("synced committed version: " + chainState.getVersion()
                + " | committed content: '" + chainState.getContent() + "'");
        System.out.println("local view: '" + localView + "'");
        long submittedCount = localPending.stream()
                .filter(op -> submittedPendingOpIds.contains(op.getOpId())).count();
        System.out.println("local pending: " + localPending.size()
                + " (submitted awaiting confirmation: " + submittedCount + ")");
    }

    private int readInt(final String prompt) {
        while (true) {
            System.out.print(prompt);
            try {
                return Integer.parseInt(scanner.nextLine().trim());
            } catch (Exception ignore) {
                System.out.println("Please enter an integer");
            }
        }
    }

    private String normalize(final String value, final String fallback) {
        return value == null || value.isBlank() ? fallback : value.trim();
    }

    private boolean sameClient(final Operation a, final Operation b) {
        return a != null && b != null
                && a.getClientId() != null
                && a.getClientId().equals(b.getClientId());
    }

    private List<Operation> collectOrderedOps(final Operation incoming, final int txIndex, final int actionIndex) {
        List<Operation> ordered = new ArrayList<>();
        if (incoming == null) {
            return ordered;
        }

        String senderId = incoming.getClientId();
        if (senderId == null || senderId.isEmpty()) {
            senderId = "unknown";
        }

        long seq = incoming.getClientSeq();

        long expected = knownClients.getOrDefault(senderId, 0L);
        if (seq > expected) {
            pendingByClient
                    .computeIfAbsent(senderId, id -> new HashMap<>())
                    .putIfAbsent(seq, new PendingOp(incoming));
            return ordered;
        }



        Map<Long, PendingOp> pending = pendingByClient.computeIfAbsent(senderId, id -> new HashMap<>());
        PendingOp current = new PendingOp(incoming);
        long next = expected;
        while (current != null) {
            knownClients.put(senderId, next + 1);
            ordered.add(serverRecOne(current.op));
            next++;
            current = pending.remove(next);
        }
        return ordered;
    }

    private long queryClientSeqFromWorldState() throws Exception {
        byte[] result = contract.evaluateTransaction("GetClientSeq", docId, clientId);
        if (result == null || result.length == 0) {
            return 0L;
        }
        String text = new String(result, StandardCharsets.UTF_8).trim();
        if (text.isEmpty()) {
            return 0L;
        }
        try {
            return Long.parseLong(text);
        } catch (NumberFormatException e) {
            return 0L;
        }
    }

    private List<String> splitIntoChunks(final String payload, final int chunkSize) {
        List<String> chunks = new ArrayList<>();
        if (payload == null || payload.isEmpty()) {
            chunks.add("");
            return chunks;
        }
        int offset = 0;
        while (offset < payload.length()) {
            int end = Math.min(payload.length(), offset + chunkSize);
            chunks.add(payload.substring(offset, end));
            offset = end;
        }
        return chunks;
    }

    // -------------------------------------------------------------------------
    // Config loading
    // -------------------------------------------------------------------------

    /**
     * Parses command-line arguments and returns the appropriate
     * {@link TestRunConfig}.
     *
     * <p>
     * Supported invocations:
     * <ul>
     * <li>(no args) — interactive mode with default Fabric connection profile.</li>
     * <li>{@code --test <config.json>} — automated test mode.</li>
     * <li>{@code --replay-log <log.json> --peer <peerId>} — replay a recorded
     * log.</li>
     * </ul>
     */
    private static TestRunConfig loadRunConfig(final String[] args) throws IOException {
        if (args == null || args.length == 0) {
            TestRunConfig config = new TestRunConfig();
            Connections.GatewayProfile profile = Connections.defaultProfile();
            config.mspId = profile.getMspId();
            config.certDirPath = profile.getCertDirPath().toString();
            config.keyDirPath = profile.getKeyDirPath().toString();
            config.tlsCertPath = profile.getTlsCertPath().toString();
            config.peerEndpoint = profile.getPeerEndpoint();
            config.overrideAuth = profile.getOverrideAuth();
            return config;
        }

        if (args.length == 2 && TEST_MODE_ARG.equals(args[0])) {
            Path configPath = Path.of(args[1]).toAbsolutePath().normalize();
            String json = Files.readString(configPath);
            TestRunConfig config = new Gson().fromJson(json, TestRunConfig.class);
            if (config == null) {
                throw new IllegalArgumentException("Invalid config json: " + configPath);
            }
            if (config.mode == null || config.mode.isBlank()) {
                config.mode = "test";
            }
            Path baseDir = configPath.getParent();
            config.certDirPath = resolvePath(baseDir, config.certDirPath).toString();
            config.keyDirPath = resolvePath(baseDir, config.keyDirPath).toString();
            config.tlsCertPath = resolvePath(baseDir, config.tlsCertPath).toString();
            return config;
        }

        if (args.length == 4 && REPLAY_LOG_ARG.equals(args[0]) && "--peer".equals(args[2])) {
            Path logPath = Path.of(args[1]).toAbsolutePath().normalize();
            String peerId = args[3];
            return loadReplayConfig(logPath, peerId);
        }

        throw new IllegalArgumentException(
                "Usage: ./gradlew run --args='--test <config.json>'"
                        + " OR --args='--replay-log <log.json> --peer <peerId>'");
    }

    /**
     * Reconstructs a {@link TestRunConfig} from a recorded operation log so a
     * previous multi-client scenario can be replayed deterministically.
     */
    private static TestRunConfig loadReplayConfig(final Path logPath, final String peerId) throws IOException {
        JsonObject root = new Gson().fromJson(Files.readString(logPath), JsonObject.class);
        if (root == null || !root.has("peers")) {
            throw new IllegalArgumentException("Invalid replay log: " + logPath);
        }

        // Find the peer entry matching peerId.
        JsonArray peers = root.getAsJsonArray("peers");
        JsonObject target = null;
        for (JsonElement element : peers) {
            if (!element.isJsonObject()) {
                continue;
            }
            JsonObject obj = element.getAsJsonObject();
            String id = obj.has("peerId") ? obj.get("peerId").getAsString() : "";
            if (peerId.equals(id)) {
                target = obj;
                break;
            }
        }

        if (target == null) {
            throw new IllegalArgumentException("Peer not found in log: " + peerId);
        }

        Connections.GatewayProfile profile = Connections.defaultProfile();
        TestRunConfig config = new TestRunConfig();
        config.mode = "test";
        config.clientId = peerId;
        config.totalOps = root.has("meta")
                && root.get("meta").isJsonObject()
                && root.get("meta").getAsJsonObject().has("totalOps")
                        ? root.get("meta").getAsJsonObject().get("totalOps").getAsInt()
                        : 0;

        // Derive a stable docId from the log file name.
        String fileName = logPath.getFileName() == null ? "replay" : logPath.getFileName().toString();
        int dot = fileName.lastIndexOf('.');
        String base = dot > 0 ? fileName.substring(0, dot) : fileName;
        config.docId = "doc_replay_" + base.replaceAll("[^a-zA-Z0-9]+", "_").toLowerCase();

        config.channelName = "mychannel";
        config.chaincodeName = "otcollab";
        config.mspId = profile.getMspId();
        config.certDirPath = profile.getCertDirPath().toString();
        config.keyDirPath = profile.getKeyDirPath().toString();
        config.tlsCertPath = profile.getTlsCertPath().toString();
        config.peerEndpoint = profile.getPeerEndpoint();
        config.overrideAuth = profile.getOverrideAuth();

        if (target.has("clocks")) {
            for (JsonElement e : target.getAsJsonArray("clocks")) {
                config.clocks.add(e.getAsInt());
            }
        }
        if (target.has("types")) {
            for (JsonElement e : target.getAsJsonArray("types")) {
                config.types.add(e.getAsString());
            }
        }
        if (target.has("positions")) {
            for (JsonElement e : target.getAsJsonArray("positions")) {
                config.positions.add(e.getAsInt());
            }
        }
        if (target.has("values")) {
            for (JsonElement e : target.getAsJsonArray("values")) {
                config.values.add(e.isJsonNull() ? "" : e.getAsString());
            }
        }

        return config;
    }

    /**
     * Resolves a path from the config file, treating relative paths as relative to
     * the config's directory.
     */
    private static Path resolvePath(final Path baseDir, final String configuredPath) {
        if (configuredPath == null || configuredPath.isBlank()) {
            throw new IllegalArgumentException("Missing path in config file");
        }
        Path path = Path.of(configuredPath);
        if (path.isAbsolute()) {
            return path.normalize();
        }
        return baseDir.resolve(path).normalize();
    }
}
