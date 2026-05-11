/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.hyperledger.fabric.samples.assettransfer;

import java.util.ArrayList;
import java.util.List;

import org.hyperledger.fabric.contract.Context;
import org.hyperledger.fabric.contract.ContractInterface;
import org.hyperledger.fabric.contract.annotation.Contact;
import org.hyperledger.fabric.contract.annotation.Contract;
import org.hyperledger.fabric.contract.annotation.Default;
import org.hyperledger.fabric.contract.annotation.Info;
import org.hyperledger.fabric.contract.annotation.License;
import org.hyperledger.fabric.contract.annotation.Transaction;
import org.hyperledger.fabric.shim.ChaincodeException;
import org.hyperledger.fabric.shim.ChaincodeStub;
import org.hyperledger.fabric.shim.ledger.KeyValue;

import com.owlike.genson.Genson;

@Contract(
        name = "otcollab",
        info = @Info(
                title = "OT + Hyperledger Fabric Collaborative Editing",
                description = "Store operation logs on-chain and maintain deterministic document state",
                version = "0.0.1",
                license = @License(
                        name = "Apache 2.0 License",
                        url = "http://www.apache.org/licenses/LICENSE-2.0.html"),
                contact = @Contact(
                        email = "ot.collab@example.com",
                        name = "Fabric Samples",
                        url = "https://hyperledger.example.com")))
@Default
public final class AssetTransfer implements ContractInterface {
    private static final String SNAP_PREFIX = "SNAP::";
    private static final String LOG_PREFIX = "LOG::";
    private static final String SNAPSHOT_PREFIX = "SNAPSHOT::";
    private static final String SNAPSHOT_CHUNK_PREFIX = "SNAPSHOT_CHUNK::";

    private final Genson genson = new Genson();

    private enum OTCollabErrors {
        DOC_NOT_FOUND,
        DOC_ALREADY_EXISTS,
        INVALID_OPERATION,
        INVALID_POSITION,
        INVALID_RANGE,
        INVALID_SNAPSHOT
    }

    @Transaction(intent = Transaction.TYPE.SUBMIT)
    public Asset InitDoc(final Context ctx, final String docId, final String initialContent) {
        ChaincodeStub stub = ctx.getStub();
        String snapKey = snapKey(docId);
        if (exists(stub, snapKey)) {
            throw new ChaincodeException("Document already exists: " + docId, OTCollabErrors.DOC_ALREADY_EXISTS.toString());
        }

        Asset initialSnapshot = new Asset(docId, "", 0);
        stub.putStringState(snapKey, genson.serialize(initialSnapshot));
        stub.setEvent("InitDoc", genson.serializeBytes(initialSnapshot));
        return initialSnapshot;
    }

    @Transaction(intent = Transaction.TYPE.SUBMIT)
    public OperationRecord SubmitOp(final Context ctx, final String docId, final String opJson) {
        ChaincodeStub stub = ctx.getStub();
        String snapKey = snapKey(docId);
        String snapJson = stub.getStringState(snapKey);
        if (snapJson == null || snapJson.isEmpty()) {
            throw new ChaincodeException("Document does not exist: " + docId, OTCollabErrors.DOC_NOT_FOUND.toString());
        }

        Operation op = genson.deserialize(opJson, Operation.class);

        // Validate operation fields exist
        if (op == null || op.getOpId() == null || op.getOpId().isEmpty()) {
            throw new ChaincodeException("op_id is required", OTCollabErrors.INVALID_OPERATION.toString());
        }
        if (op.getType() == null) {
            throw new ChaincodeException("type is required", OTCollabErrors.INVALID_OPERATION.toString());
        }

        // Do not read operation range here to avoid PHANTOM_READ_CONFLICT under concurrent submits.
        // Ordering and transform are handled client-side per committed block.
        long committedOrder = op.getTimestamp();
        if (stub.getTxTimestamp() != null) {
            committedOrder = stub.getTxTimestamp().getEpochSecond() * 1_000_000_000L + stub.getTxTimestamp().getNano();
        }
        Operation committedOp = new Operation(
            op.getOpId(),
            op.getClientId(),
            op.getType(),
            op.getPosition(),
            op.getValue(),
            op.getTimestamp(),
            op.getAck());
        OperationRecord record = new OperationRecord(docId, committedOp, committedOrder, stub.getTxId());

        // Write to operation log (append-only key, no shared-key write conflict)
        stub.putStringState(logKey(docId, committedOrder, stub.getTxId(), op.getOpId()), genson.serialize(record));
        stub.setEvent("SubmitOp::" + docId, genson.serializeBytes(record));

        return record;
    }

    @Transaction(intent = Transaction.TYPE.EVALUATE)
    public Asset QueryState(final Context ctx, final String docId) {
        ChaincodeStub stub = ctx.getStub();
        String marker = stub.getStringState(snapKey(docId));
        if (marker == null || marker.isEmpty()) {
            throw new ChaincodeException("Document does not exist: " + docId, OTCollabErrors.DOC_NOT_FOUND.toString());
        }

        long version = 0;
        for (KeyValue kv : stub.getStateByRange(logPrefix(docId), logPrefixEnd(docId))) {
            version++;
        }

        return new Asset(docId, "", version);
    }

    @Transaction(intent = Transaction.TYPE.EVALUATE)
    public String QueryAllOps(final Context ctx, final String docId) {
        String marker = ctx.getStub().getStringState(snapKey(docId));
        if (marker == null || marker.isEmpty()) {
            throw new ChaincodeException("Document does not exist: " + docId, OTCollabErrors.DOC_NOT_FOUND.toString());
        }

        ChaincodeStub stub = ctx.getStub();
        String startKey = logPrefix(docId);
        String endKey = logPrefixEnd(docId);

        List<OperationRecord> ops = new ArrayList<>();
        for (KeyValue kv : stub.getStateByRange(startKey, endKey)) {
            ops.add(genson.deserialize(kv.getStringValue(), OperationRecord.class));
        }

        return genson.serialize(ops);
    }
    
    @Transaction(intent = Transaction.TYPE.SUBMIT)
    public String SaveSnapshot(final Context ctx, final String docId, final String snapshotJson) {
        ChaincodeStub stub = ctx.getStub();
        String marker = stub.getStringState(snapKey(docId));
        if (marker == null || marker.isEmpty()) {
            throw new ChaincodeException("Document does not exist: " + docId, OTCollabErrors.DOC_NOT_FOUND.toString());
        }

        DocumentSnapshot snapshot = genson.deserialize(snapshotJson, DocumentSnapshot.class);
        if (snapshot == null || snapshot.getDocId() == null || !docId.equals(snapshot.getDocId())) {
            throw new ChaincodeException("Invalid snapshot payload", OTCollabErrors.INVALID_SNAPSHOT.toString());
        }
        // lastLogCursorKey removed; no longer required

        String snapshotKey = snapshotKey(docId);
        String existingJson = stub.getStringState(snapshotKey);
        if (existingJson != null && !existingJson.isEmpty()) {
            DocumentSnapshot existing = genson.deserialize(existingJson, DocumentSnapshot.class);
            if (existing != null && snapshot.getVersion() <= existing.getVersion()) {
                throw new ChaincodeException(
                        "Stale snapshot version: " + snapshot.getVersion(),
                        OTCollabErrors.INVALID_SNAPSHOT.toString());
            }
        }
        stub.putStringState(snapshotKey, genson.serialize(snapshot));
        stub.setEvent("SaveSnapshot", genson.serializeBytes(snapshot));
        return snapshotKey;
    }

    @Transaction(intent = Transaction.TYPE.SUBMIT)
    public String SaveSnapshotChunk(final Context ctx,
                                    final String docId,
                                    final String snapshotId,
                                    final int index,
                                    final int totalChunks,
                                    final String chunkPayload) {
        ChaincodeStub stub = ctx.getStub();
        String marker = stub.getStringState(snapKey(docId));
        if (marker == null || marker.isEmpty()) {
            throw new ChaincodeException("Document does not exist: " + docId, OTCollabErrors.DOC_NOT_FOUND.toString());
        }
        if (snapshotId == null || snapshotId.isEmpty()) {
            throw new ChaincodeException("snapshotId is required", OTCollabErrors.INVALID_SNAPSHOT.toString());
        }
        if (index < 0 || totalChunks <= 0 || index >= totalChunks) {
            throw new ChaincodeException("Invalid chunk index", OTCollabErrors.INVALID_SNAPSHOT.toString());
        }
        String key = snapshotChunkKey(docId, snapshotId, index);
        stub.putStringState(key, chunkPayload == null ? "" : chunkPayload);
        return key;
    }

    @Transaction(intent = Transaction.TYPE.SUBMIT)
    public String CommitSnapshotPointer(final Context ctx, final String docId, final String pointerJson) {
        ChaincodeStub stub = ctx.getStub();
        String marker = stub.getStringState(snapKey(docId));
        if (marker == null || marker.isEmpty()) {
            throw new ChaincodeException("Document does not exist: " + docId, OTCollabErrors.DOC_NOT_FOUND.toString());
        }

        SnapshotPointer pointer = genson.deserialize(pointerJson, SnapshotPointer.class);
        if (pointer == null || pointer.getDocId() == null || !docId.equals(pointer.getDocId())) {
            throw new ChaincodeException("Invalid snapshot pointer", OTCollabErrors.INVALID_SNAPSHOT.toString());
        }
        if (!pointer.isChunked() || pointer.getTotalChunks() <= 0) {
            throw new ChaincodeException("Invalid snapshot pointer", OTCollabErrors.INVALID_SNAPSHOT.toString());
        }

        String snapshotKey = snapshotKey(docId);
        String existingJson = stub.getStringState(snapshotKey);
        if (existingJson != null && !existingJson.isEmpty()) {
            try {
                SnapshotPointer existing = genson.deserialize(existingJson, SnapshotPointer.class);
                if (existing != null && existing.isChunked() && pointer.getVersion() <= existing.getVersion()) {
                    throw new ChaincodeException(
                            "Stale snapshot version: " + pointer.getVersion(),
                            OTCollabErrors.INVALID_SNAPSHOT.toString());
                }
            } catch (Exception ignore) {
                // Existing value might be a full snapshot JSON; ignore and overwrite.
            }
        }

        stub.putStringState(snapshotKey, genson.serialize(pointer));
        return snapshotKey;
    }

    @Transaction(intent = Transaction.TYPE.EVALUATE)
    public String GetLatestSnapshot(final Context ctx, final String docId) {
        ChaincodeStub stub = ctx.getStub();
        String marker = stub.getStringState(snapKey(docId));
        if (marker == null || marker.isEmpty()) {
            throw new ChaincodeException("Document does not exist: " + docId, OTCollabErrors.DOC_NOT_FOUND.toString());
        }

        String snapshotJson = stub.getStringState(snapshotKey(docId));
        if (snapshotJson == null || snapshotJson.isEmpty()) {
            return "";
        }

        try {
            SnapshotPointer pointer = genson.deserialize(snapshotJson, SnapshotPointer.class);
            if (pointer != null && pointer.isChunked() && pointer.getTotalChunks() > 0) {
                StringBuilder builder = new StringBuilder();
                for (int i = 0; i < pointer.getTotalChunks(); i++) {
                    String chunk = stub.getStringState(snapshotChunkKey(docId, pointer.getSnapshotId(), i));
                    builder.append(chunk == null ? "" : chunk);
                }
                return builder.toString();
            }
        } catch (Exception ignore) {
            // Fall through to legacy full snapshot JSON.
        }

        return snapshotJson;
    }

    private boolean exists(final ChaincodeStub stub, final String key) {
        String value = stub.getStringState(key);
        return value != null && !value.isEmpty();
    }

    private String snapKey(final String docId) {
        return SNAP_PREFIX + docId;
    }

    private String snapshotKey(final String docId) {
        return SNAPSHOT_PREFIX + docId;
    }

    private String snapshotChunkKey(final String docId, final String snapshotId, final int index) {
        return String.format("%s%s::%s::%06d", SNAPSHOT_CHUNK_PREFIX, docId, snapshotId, index);
    }

    private String logKey(final String docId, final long submittedTs, final String txId, final String opId) {
        return String.format("%s%s::%020d::%s::%s", LOG_PREFIX, docId, submittedTs,
                txId == null ? "" : txId, opId == null ? "" : opId);
    }

    private String logPrefix(final String docId) {
        return String.format("%s%s::", LOG_PREFIX, docId);
    }

    private String logPrefixEnd(final String docId) {
        return String.format("%s%s::\uffff", LOG_PREFIX, docId);
    }
}
