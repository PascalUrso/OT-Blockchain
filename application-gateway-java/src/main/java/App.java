/*
 * SPDX-License-Identifier: Apache-2.0
 */

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.reflect.TypeToken;
import org.hyperledger.fabric.client.Contract;
import org.hyperledger.fabric.client.Gateway;
import org.hyperledger.fabric.client.Hash;
import org.hyperledger.fabric.client.Network;

import java.io.IOException;
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
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;

public final class App {
    private static final int MAX_SUBMIT_RETRIES = 3;
    private static final String TEST_MODE_ARG = "--test";
    private static final String REPLAY_LOG_ARG = "--replay-log";

    private final Gson gson = new GsonBuilder().setPrettyPrinting().create();
    private final Scanner scanner = new Scanner(System.in);
    private final Random random = new Random();

    private final Network network;
    private final Contract contract;
    private final TestRunConfig runConfig;
    private final boolean testMode;

    private String clientId;
    private String docId;
    private DocumentState chainState;
    private String committedView;
    private String localView;
    private String lastLogCursorKey = "";
    private long lastSyncedVersion;
    private long localAck;
    private int localClock;
    private final List<Integer> testClocks = new ArrayList<>();
    private final List<ScheduledOp> scheduledOps = new ArrayList<>();
    private int nextScheduledOpIndex = 0;
    private final List<Operation> committedHistory = new ArrayList<>();
    private final Map<String, List<Operation>> clientBuffers = new HashMap<>();
    private final Set<String> knownClients = new HashSet<>();
    private final List<Operation> localPending = new CopyOnWriteArrayList<>();
    private final Set<String> submittedPendingOpIds = new HashSet<>();

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

    public static void main(final String[] args) throws Exception {
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
        this.network = gateway.getNetwork(runConfig.channelName);
        this.contract = network.getContract(runConfig.chaincodeName);
    }

    private void run() throws Exception {
        if (testMode) {
            clientId = normalize(runConfig.clientId, "userA");
            docId = normalize(runConfig.docId, "doc_1");
            testClocks.clear();
            scheduledOps.clear();
            nextScheduledOpIndex = 0;
            if (runConfig.clocks != null) {
                testClocks.addAll(runConfig.clocks);
            }
            loadScheduledOps();
            System.out.println("Running in test mode, client=" + clientId + ", doc=" + docId + ", clocks=" + testClocks);
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

        bootstrap();
        startBlockListener();

        if (testMode) {
            runTestMode();
            return;
        }

        while (true) {
            printStatus();
            System.out.println("\n1) INSERT(local pending)  2) DELETE(local pending)  3) UPDATE(local pending)");
            System.out.println("4) submit 1 local pending  5) submit all local pending  6) manually sync with chain");
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
                    syncFromChain(true);
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
                    System.out.println("无效输入");
            }
        }
    }

    private void runTestMode() throws Exception {
        long timeoutSeconds = runConfig.testTimeoutSeconds <= 0 ? 180 : runConfig.testTimeoutSeconds;
        long pollMillis = runConfig.pollMillis <= 0 ? 300 : runConfig.pollMillis;
        long deadline = System.currentTimeMillis() + timeoutSeconds * 1000;

        System.out.println("Test mode started, scheduledOps=" + scheduledOps.size()
                + ", timeout=" + timeoutSeconds + "s, poll=" + pollMillis + "ms");
        int totalOps = runConfig.totalOps;
        while (lastSyncedVersion < totalOps) {
            TimeUnit.MILLISECONDS.sleep(pollMillis);
        }

        syncFromChain(true);
        System.out.println("Test mode finished. dispatched=" + nextScheduledOpIndex + "/" + scheduledOps.size());
    }

    private void loadScheduledOps() {
        if (testClocks.isEmpty()) {
            return;
        }

        List<String> cfgTypes = runConfig.types == null ? new ArrayList<>() : runConfig.types;
        List<Integer> cfgPositions = runConfig.positions == null ? new ArrayList<>() : runConfig.positions;
        List<String> cfgValues = runConfig.values == null ? new ArrayList<>() : runConfig.values;

        boolean hasDetailed = !cfgTypes.isEmpty() && cfgTypes.size() == testClocks.size();

        for (int i = 0; i < testClocks.size(); i++) {
            int clock = testClocks.get(i);
            if (hasDetailed) {
                String type = cfgTypes.get(i);
                Integer position = i < cfgPositions.size() ? cfgPositions.get(i) : null;
                String value = i < cfgValues.size() ? cfgValues.get(i) : "";
                scheduledOps.add(new ScheduledOp(clock, type, position, value));
            } else {
                scheduledOps.add(new ScheduledOp(clock, null, null, null));
            }
        }

        scheduledOps.sort((a, b) -> Integer.compare(a.clock, b.clock));
    }

    private void bootstrap() throws Exception {
        try {
            contract.submitTransaction("InitDoc", docId, "");
        } catch (Exception e) {
            // already initialized by another client
        }

        committedView = "";
        chainState = new DocumentState(docId, committedView, 0);
        lastSyncedVersion = chainState.getVersion();
        localView = committedView;
        localAck = 0;
        localClock = 0;
        knownClients.clear();
        knownClients.add(clientId);
        clientBuffers.clear();
        clientBuffers.put(clientId, new ArrayList<>());
        rebuildCommittedFromLedger();
        System.out.println("Initialized: version=" + chainState.getVersion() + ", content='" + chainState.getContent() + "'");
    }

    private void rebuildCommittedFromLedger() throws Exception {
        committedView = "";
        committedHistory.clear();
        localPending.clear();
        submittedPendingOpIds.clear();
        lastLogCursorKey = "";
        submitScheduledOpsAtClock(localClock);
        syncFromChain(true);
    }

    private void startBlockListener() {
        Thread t = new Thread(() -> {
            try (var events = network.newBlockEventsRequest().build().getEvents()) {
                events.forEachRemaining(event -> {
                    try {
                        System.out.println("New block received: " + event.getHeader().getNumber());
                        syncFromChain(true);
                    } catch (Exception e) {
                        System.out.println("Block sync failed: " + e.getMessage());
                    }
                });
            } catch (Exception e) {
                System.out.println("Block listener stopped: " + e.getMessage());
            }
        });
        t.setDaemon(true);
        t.start();
    }

    private void stageLocalPending(final OperationType type) {
        int pos = readInt("position: ");
        String value = "";
        if (type == OperationType.insert || type == OperationType.update) {
            System.out.print("value: ");
            value = scanner.nextLine();
        }

        Operation op = new Operation(
                UUID.randomUUID().toString(),
                clientId,
                type,
                pos,
                value,
            Instant.now().toEpochMilli(),
            0);


        try {
            localView = OTEngine.apply(localView, op);
            localPending.add(op);
            System.out.println("Added to local pending, op_id=" + op.getOpId());
        } catch (Exception e) {
            System.out.println("Failed to apply operation locally: " + e.getMessage());
        }
    }

    private void submitNextPending() {
        if (!hasUnsubmittedPending()) {
            System.out.println("No unsubmitted local pending operations");
            return;
        }

        for (int attempt = 1; attempt <= MAX_SUBMIT_RETRIES; attempt++) {
            Operation candidate = getNextUnsubmittedPending();
            if (candidate == null) {
                System.out.println("No unsubmitted local pending operations");
                return;
            }

            Operation candidateForSubmit = withAck(candidate, localAck);

            try {
                contract.submitTransaction("SubmitOp", docId, gson.toJson(candidateForSubmit));
                submittedPendingOpIds.add(candidateForSubmit.getOpId());
                System.out.println("Tx successfully sent, op_id=" + candidateForSubmit.getOpId() + ", ack=" + localAck);
                localAck = 0;
                return;
            } catch (Exception e) {
                if (isMvccConflict(e) && attempt < MAX_SUBMIT_RETRIES) {
                    System.out.println("MVCC conflict detected, syncing and retrying (" + attempt + "/" + MAX_SUBMIT_RETRIES + ")");
                    try {
                        syncFromChain(true);
                    } catch (Exception syncException) {
                        System.out.println("Sync failed: " + syncException.getMessage());
                    }
                    continue;
                }
                System.out.println("Submit failed: " + e.getMessage());
                return;
            }
        }
    }

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
            0);
    }

    private void stagePendingAndSubmit(final int clock,
                                       final OperationType type,
                                       final Integer requestedPosition,
                                       final String requestedValue) {
        int len = localView == null ? 0 : localView.length();
        OperationType finalType = type;
        int finalPos;
        String finalValue = requestedValue == null ? "" : requestedValue;

        if (finalType == OperationType.delete || finalType == OperationType.update) {
            if (len <= 0) {
                finalType = OperationType.insert;
            }
        }

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

        Operation op = new Operation(
                UUID.randomUUID().toString(),
                clientId,
                finalType,
                finalPos,
                finalValue,
                Instant.now().toEpochMilli(),
                0);

        try {
            localView = OTEngine.apply(localView, op);
            localPending.add(op);
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

    private void clientRec(final List<Operation> transformedBlockOps) {
        for (Operation op : transformedBlockOps) {
            localClock++;
            // 客户端立即收到ot转换后的操作，srec要么是0要么是1。如果这个操作来自本client，则ack是1.删除第一个操作，无需啊apply。
            if (!localPending.isEmpty() && op.getClientId().equals(clientId)) {
                localPending.subList(0, 1).clear();
                submittedPendingOpIds.remove(op.getOpId());
                submitScheduledOpsAtClock(localClock);
                continue;
            }
            localAck++;
            
            for(int i = 0; i < localPending.size(); i++) {
                Operation pendingOp = localPending.get(i);
                Operation pendingPrime = OTEngine.transform(pendingOp, op);
                Operation opPrime = OTEngine.transform(op, pendingOp);
                op = opPrime;
                localPending.set(i, pendingPrime);
            }
            localView = OTEngine.apply(localView, op);
            submitScheduledOpsAtClock(localClock);
        }
    }

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
    private synchronized void syncFromChain(final boolean verbose) throws Exception {
        List<OperationRecord> newCommittedOps = queryOpsAfter(lastLogCursorKey);

        if (newCommittedOps.isEmpty()) {
            return;
        }

        //server端
        List<Operation> transformedBlockOps = new ArrayList<>();
        for (OperationRecord record : newCommittedOps) {
            System.out.println("Replaying committed op from ledger: " + record.getOperation().getOpId());
            Operation op = serverRec(record.getOperation());
            transformedBlockOps.add(op);
        }
        
        lastLogCursorKey = buildLogCursorKey(newCommittedOps.get(newCommittedOps.size() - 1));

        //lastSyncedVersion = committedHistory.size();
        chainState = new DocumentState(docId, committedView, lastSyncedVersion);

        //client端
        clientRec(transformedBlockOps);

        if (verbose) {
            System.out.println("Synced to chain version=" + chainState.getVersion());
            printStatus();
        }
    }

    private List<OperationRecord> queryAllOps() throws Exception {
        byte[] result = contract.evaluateTransaction("QueryAllOps", docId);
        return gson.fromJson(new String(result, StandardCharsets.UTF_8), new TypeToken<List<OperationRecord>>() {
        }.getType());
    }

    private List<OperationRecord> queryOpsAfter(final String afterKeyExclusive) throws Exception {
        String cursor = afterKeyExclusive == null ? "" : afterKeyExclusive;
        byte[] result = contract.evaluateTransaction("QueryOpsAfter", docId, cursor);
        return gson.fromJson(new String(result, StandardCharsets.UTF_8), new TypeToken<List<OperationRecord>>() {
        }.getType());
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
        return a != null
                && b != null
                && a.getClientId() != null
                && a.getClientId().equals(b.getClientId());
    }

    private List<Operation> getClientBuffer(final String targetClientId) {
        return clientBuffers.computeIfAbsent(targetClientId, this::buildInitialBufferForClient);
    }

    private List<Operation> buildInitialBufferForClient(final String targetClientId) {
        long ackToSkip = 0;
        for (int i = committedHistory.size() - 1; i >= 0; i--) {
            Operation op = committedHistory.get(i);
            if (op.getClientId() != null && op.getClientId().equals(targetClientId)) {
                ackToSkip = op.getAck();
                break;
            }
        }

        long skipped = 0;
        List<Operation> initial = new ArrayList<>();
        for (Operation committed : committedHistory) {
            if (committed.getClientId() != null && committed.getClientId().equals(targetClientId)) {
                continue;
            }

            if (skipped < ackToSkip) {
                skipped++;
                continue;
            }

            initial.add(committed);
        }
        return initial;
    }

    private Operation withAck(final Operation op, final long ack) {
        return new Operation(
                op.getOpId(),
                op.getClientId(),
                op.getType(),
                op.getPosition(),
                op.getValue(),
                op.getTimestamp(),
                ack);
    }

    private int normalizeAck(final long ackValue, final int bufferSize) {
        if (ackValue <= 0) {
            return 0;
        }

        if (ackValue > Integer.MAX_VALUE) {
            return bufferSize;
        }

        return Math.min((int) ackValue, bufferSize);
    }

    private Operation serverRec(final Operation incomingRaw) {
        String senderId = incomingRaw.getClientId();
        if (senderId == null || senderId.isEmpty()) {
            senderId = "unknown";
        }

        knownClients.add(senderId);
        knownClients.add(clientId);

        List<Operation> senderBuffer = getClientBuffer(senderId);
        int ack = normalizeAck(incomingRaw.getAck(), senderBuffer.size());
        if (ack > 0) {
            senderBuffer.subList(0, ack).clear();
        }

        Operation incoming = incomingRaw;
        for (int i = 0; i < senderBuffer.size(); i++) {
            Operation bufferOp = senderBuffer.get(i);
            if (sameClient(bufferOp, incoming)) {
                continue;
            }

            Operation bufferPrime = OTEngine.transform(bufferOp, incoming);
            Operation incomingPrime = OTEngine.transform(incoming, bufferOp);
            senderBuffer.set(i, bufferPrime);
            incoming = incomingPrime;
        }

        committedView = OTEngine.apply(committedView, incoming);
        committedHistory.add(incoming);
        lastSyncedVersion ++;

        List<String> recipients = new ArrayList<>(knownClients);
        for (String receiverId : recipients) {
            if (receiverId == null || receiverId.equals(senderId)) {
                continue;
            }
            getClientBuffer(receiverId).add(incoming);
        }

        return incoming;
    }

    private String buildLogCursorKey(final OperationRecord record) {
        if (record == null || record.getOperation() == null) {
            return "";
        }

        return String.format("LOG::%s::%020d::%s::%s",
                docId,
                record.getCommittedVersion(),
                record.getTxId() == null ? "" : record.getTxId(),
                record.getOperation().getOpId() == null ? "" : record.getOperation().getOpId());
    }

    private void printStatus() {
        // List<Operation> pool = pendingPool.getPendingSorted(docId);
        // pool.sort(Comparator.comparingLong(Operation::getTimestamp).thenComparing(Operation::getClientId));

        System.out.println("\n==== Current Status ====");
        System.out.println("synced committed version: " + chainState.getVersion() + " | committed content: '" + chainState.getContent() + "'");
        System.out.println("local view: '" + localView + "'");
        long submittedCount = localPending.stream().filter(op -> submittedPendingOpIds.contains(op.getOpId())).count();
        // System.out.println("本地pending数量: " + localPending.size() + " (已提交待确认: " + submittedCount + ") | 全局pending数量: " + pool.size());
        System.out.println("local pending number: " + localPending.size() + " (ops submitted waiting to be confirmed: " + submittedCount + ")");
    }

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

            config.mode = "test";
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

        throw new IllegalArgumentException("Usage: ./gradlew run --args='--test <config.json>' OR --args='--replay-log <log.json> --peer <peerId>'");
    }

    private static TestRunConfig loadReplayConfig(final Path logPath, final String peerId) throws IOException {
        JsonObject root = new Gson().fromJson(Files.readString(logPath), JsonObject.class);
        if (root == null || !root.has("peers")) {
            throw new IllegalArgumentException("Invalid replay log: " + logPath);
        }

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
        config.totalOps = root.has("meta") && root.get("meta").isJsonObject() && root.get("meta").getAsJsonObject().has("totalOps") ? root.get("meta").getAsJsonObject().get("totalOps").getAsInt() : 0;
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