/*
 * SPDX-License-Identifier: Apache-2.0
 */

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.reflect.TypeToken;
import org.hyperledger.fabric.client.Contract;
import org.hyperledger.fabric.client.Gateway;
import org.hyperledger.fabric.client.Hash;
import org.hyperledger.fabric.client.Network;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;

public final class App {
    private static final String CHANNEL_NAME = "mychannel";
    private static final String CHAINCODE_NAME = "otcollab";
    private static final int MAX_SUBMIT_RETRIES = 3;

    private final Gson gson = new GsonBuilder().setPrettyPrinting().create();
    private final Scanner scanner = new Scanner(System.in);

    private final Network network;
    private final Contract contract;

    private String clientId;
    private String docId;
    private DocumentState chainState;
    private String committedView;
    private String localView;
    private String lastLogCursorKey = "";
    private long lastSyncedVersion;
    private final List<Operation> committedHistory = new ArrayList<>();
    private final Map<String, List<Operation>> clientBuffers = new HashMap<>();
    private final Set<String> knownClients = new HashSet<>();
    private final List<Operation> localPending = new CopyOnWriteArrayList<>();
    private final Set<String> submittedPendingOpIds = new HashSet<>();

    public static void main(final String[] args) throws Exception {
        var grpcChannel = Connections.newGrpcConnection();
        var builder = Gateway.newInstance()
                .identity(Connections.newIdentity())
                .signer(Connections.newSigner())
                .hash(Hash.SHA256)
                .connection(grpcChannel)
                .evaluateOptions(options -> options.withDeadlineAfter(5, TimeUnit.SECONDS))
                .endorseOptions(options -> options.withDeadlineAfter(15, TimeUnit.SECONDS))
                .submitOptions(options -> options.withDeadlineAfter(5, TimeUnit.SECONDS))
                .commitStatusOptions(options -> options.withDeadlineAfter(1, TimeUnit.MINUTES));

        try (var gateway = builder.connect()) {
            new App(gateway).run();
        } finally {
            grpcChannel.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
        }
    }

    public App(final Gateway gateway) {
        this.network = gateway.getNetwork(CHANNEL_NAME);
        this.contract = network.getContract(CHAINCODE_NAME);
    }

    private void run() throws Exception {
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

        bootstrap();
        startBlockListener();

        while (true) {
            printStatus();
            System.out.println("\n1) INSERT(local pending)  2) DELETE(local pending)  3) UPDATE(local pending)");
            System.out.println("4) submit 1 local pending  5) submit all local pending  6) manually sync with chain");
            System.out.println("8) exit");
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
                case "8":
                    return;
                default:
                    System.out.println("Invalid input");
            }
        }
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

            int selfBufferSize = getClientBuffer(clientId).size();
            Operation candidateForSubmit = withAck(candidate, selfBufferSize);

            try {
                contract.submitTransaction("SubmitOp", docId, gson.toJson(candidateForSubmit));
                submittedPendingOpIds.add(candidateForSubmit.getOpId());
                System.out.println("Tx successfully sent, op_id=" + candidateForSubmit.getOpId() + ", ack=" + selfBufferSize);
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

    private synchronized void syncFromChain(final boolean verbose) throws Exception {
        List<OperationRecord> newCommittedOps = queryOpsAfter(lastLogCursorKey);

        if (newCommittedOps.isEmpty()) {
            return;
        }

        // Server side: replay committed ops through the OT transform pipeline
        List<Operation> transformedBlockOps = new ArrayList<>();
        for (OperationRecord record : newCommittedOps) {
            System.out.println("Replaying committed op from ledger: " + record.getOperation().getOpId());
            Operation op = applyCommittedOperation(record.getOperation());
            transformedBlockOps.add(op);
        }

        lastLogCursorKey = buildLogCursorKey(newCommittedOps.get(newCommittedOps.size() - 1));
        lastSyncedVersion = committedHistory.size();
        chainState = new DocumentState(docId, committedView, lastSyncedVersion);

        // Client side: transform local pending ops against the newly committed ops,
        // then apply the (transformed) remote ops to the local view.
        // If a committed op originated from this client, it means our pending op was
        // confirmed — remove it from the pending list instead of re-applying it.
        List<Operation> applyOps = new ArrayList<>();
        for (Operation op : transformedBlockOps) {
            if (!localPending.isEmpty() && op.getClientId().equals(clientId)) {
                localPending.subList(0, 1).clear();
                submittedPendingOpIds.remove(op.getOpId());
                continue;
            }
            for (int i = 0; i < localPending.size(); i++) {
                Operation pendingOp = localPending.get(i);
                Operation pendingPrime = OTEngine.transform(pendingOp, op);
                Operation opPrime = OTEngine.transform(op, pendingOp);
                op = opPrime;
                localPending.set(i, pendingPrime);
            }
            applyOps.add(op);
        }
        for (Operation op : applyOps) {
            localView = OTEngine.apply(localView, op);
        }

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

    private Operation applyCommittedOperation(final Operation incomingRaw) {
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
        System.out.println("\n==== Current Status ====");
        System.out.println("synced committed version: " + chainState.getVersion() + " | committed content: '" + chainState.getContent() + "'");
        System.out.println("local view: '" + localView + "'");
        long submittedCount = localPending.stream().filter(op -> submittedPendingOpIds.contains(op.getOpId())).count();
        System.out.println("local pending number: " + localPending.size() + " (ops submitted waiting to be confirmed: " + submittedCount + ")");
    }
}
