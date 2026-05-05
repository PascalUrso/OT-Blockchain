/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.hyperledger.fabric.samples.assettransfer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.ThrowableAssert.catchThrowable;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.hyperledger.fabric.contract.Context;
import org.hyperledger.fabric.shim.ChaincodeException;
import org.hyperledger.fabric.shim.ChaincodeStub;
import org.hyperledger.fabric.shim.ledger.KeyValue;
import org.hyperledger.fabric.shim.ledger.QueryResultsIterator;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentMatchers;

import com.owlike.genson.Genson;

public final class AssetTransferTest {

    // -------------------------------------------------------------------------
    // Test doubles
    // -------------------------------------------------------------------------

    private static final class MockKeyValue implements KeyValue {
        private final String key;
        private final String value;

        MockKeyValue(final String key, final String value) {
            this.key = key;
            this.value = value;
        }

        @Override
        public String getKey() {
            return key;
        }

        @Override
        public String getStringValue() {
            return value;
        }

        @Override
        public byte[] getValue() {
            return value.getBytes();
        }
    }

    private static final class MockQueryResultsIterator implements QueryResultsIterator<KeyValue> {
        private final List<KeyValue> items;

        MockQueryResultsIterator(final List<KeyValue> items) {
            this.items = items;
        }

        @Override
        public Iterator<KeyValue> iterator() {
            return items.iterator();
        }

        @Override
        public void close() {
            // no-op
        }
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    private static final Genson GENSON = new Genson();

    /** Existing doc marker — any non-empty string is enough for the chaincode exists() check. */
    private static final String DOC_EXISTS = "{\"content\":\"\",\"docId\":\"doc1\",\"version\":0}";

    /** Serialises an Operation to JSON using the same Genson instance as the chaincode. */
    private static String opJson(final String opId, final String clientId,
                                  final OperationType type, final int pos,
                                  final String value, final long ack) {
        Operation op = new Operation(opId, clientId, type, pos, value, 1_000L, ack);
        return GENSON.serialize(op);
    }

    // -------------------------------------------------------------------------
    // InitDoc
    // -------------------------------------------------------------------------

    @Nested
    class InitDocTransaction {

        @Test
        public void createsDocumentSuccessfully() {
            AssetTransfer contract = new AssetTransfer();
            Context ctx = mock(Context.class);
            ChaincodeStub stub = mock(ChaincodeStub.class);
            when(ctx.getStub()).thenReturn(stub);
            when(stub.getStringState("SNAP::doc1")).thenReturn("");

            Asset result = contract.InitDoc(ctx, "doc1", "");

            assertThat(result.getDocId()).isEqualTo("doc1");
            assertThat(result.getContent()).isEqualTo("");
            assertThat(result.getVersion()).isEqualTo(0);
            verify(stub).putStringState(ArgumentMatchers.eq("SNAP::doc1"), ArgumentMatchers.anyString());
        }

        @Test
        public void throwsWhenDocumentAlreadyExists() {
            AssetTransfer contract = new AssetTransfer();
            Context ctx = mock(Context.class);
            ChaincodeStub stub = mock(ChaincodeStub.class);
            when(ctx.getStub()).thenReturn(stub);
            when(stub.getStringState("SNAP::doc1")).thenReturn(DOC_EXISTS);

            Throwable thrown = catchThrowable(() -> contract.InitDoc(ctx, "doc1", ""));

            assertThat(thrown)
                    .isInstanceOf(ChaincodeException.class)
                    .hasMessageContaining("doc1");
        }
    }

    // -------------------------------------------------------------------------
    // SubmitOp
    // -------------------------------------------------------------------------

    @Nested
    class SubmitOpTransaction {

        @Test
        public void throwsWhenDocumentDoesNotExist() {
            AssetTransfer contract = new AssetTransfer();
            Context ctx = mock(Context.class);
            ChaincodeStub stub = mock(ChaincodeStub.class);
            when(ctx.getStub()).thenReturn(stub);
            when(stub.getStringState("SNAP::doc1")).thenReturn("");

            Throwable thrown = catchThrowable(() ->
                    contract.SubmitOp(ctx, "doc1", opJson("op-1", "userA", OperationType.insert, 0, "a", 0)));

            assertThat(thrown)
                    .isInstanceOf(ChaincodeException.class)
                    .hasMessageContaining("doc1");
        }

        @Test
        public void throwsWhenOpIdIsMissing() {
            AssetTransfer contract = new AssetTransfer();
            Context ctx = mock(Context.class);
            ChaincodeStub stub = mock(ChaincodeStub.class);
            when(ctx.getStub()).thenReturn(stub);
            when(stub.getStringState("SNAP::doc1")).thenReturn(DOC_EXISTS);

            // opId is blank
            Throwable thrown = catchThrowable(() ->
                    contract.SubmitOp(ctx, "doc1", opJson("", "userA", OperationType.insert, 0, "a", 0)));

            assertThat(thrown).isInstanceOf(ChaincodeException.class);
        }

        @Test
        public void throwsWhenTypeIsMissing() {
            AssetTransfer contract = new AssetTransfer();
            Context ctx = mock(Context.class);
            ChaincodeStub stub = mock(ChaincodeStub.class);
            when(ctx.getStub()).thenReturn(stub);
            when(stub.getStringState("SNAP::doc1")).thenReturn(DOC_EXISTS);

            // no "type" field → type will be null after deserialization
            String json = "{\"opId\":\"op-1\",\"clientId\":\"userA\",\"position\":0,\"value\":\"a\",\"timestamp\":1000,\"ack\":0}";

            Throwable thrown = catchThrowable(() -> contract.SubmitOp(ctx, "doc1", json));

            assertThat(thrown).isInstanceOf(ChaincodeException.class);
        }

        @Test
        public void appendsOperationRecordToLedger() {
            AssetTransfer contract = new AssetTransfer();
            Context ctx = mock(Context.class);
            ChaincodeStub stub = mock(ChaincodeStub.class);
            when(ctx.getStub()).thenReturn(stub);
            when(stub.getStringState("SNAP::doc1")).thenReturn(DOC_EXISTS);
            when(stub.getTxTimestamp()).thenReturn(Instant.ofEpochSecond(1_000_000));
            when(stub.getTxId()).thenReturn("tx-abc");

            OperationRecord record = contract.SubmitOp(ctx, "doc1",
                    opJson("op-1", "userA", OperationType.insert, 0, "a", 0));

            assertThat(record.getOperation().getOpId()).isEqualTo("op-1");
            assertThat(record.getOperation().getClientId()).isEqualTo("userA");
            assertThat(record.getTxId()).isEqualTo("tx-abc");
            // A log entry must have been written under a key that starts with LOG::doc1
            verify(stub).putStringState(
                    ArgumentMatchers.contains("LOG::doc1"),
                    ArgumentMatchers.anyString());
        }
    }

    // -------------------------------------------------------------------------
    // QueryAllOps
    // -------------------------------------------------------------------------

    @Nested
    class QueryAllOpsTransaction {

        @Test
        public void throwsWhenDocumentDoesNotExist() {
            AssetTransfer contract = new AssetTransfer();
            Context ctx = mock(Context.class);
            ChaincodeStub stub = mock(ChaincodeStub.class);
            when(ctx.getStub()).thenReturn(stub);
            when(stub.getStringState("SNAP::doc1")).thenReturn("");

            Throwable thrown = catchThrowable(() -> contract.QueryAllOps(ctx, "doc1"));

            assertThat(thrown)
                    .isInstanceOf(ChaincodeException.class)
                    .hasMessageContaining("doc1");
        }

        @Test
        public void returnsEmptyArrayWhenNoOpsCommitted() {
            AssetTransfer contract = new AssetTransfer();
            Context ctx = mock(Context.class);
            ChaincodeStub stub = mock(ChaincodeStub.class);
            when(ctx.getStub()).thenReturn(stub);
            when(stub.getStringState("SNAP::doc1")).thenReturn(DOC_EXISTS);
            when(stub.getStateByRange("LOG::doc1::", "LOG::doc1::￿"))
                    .thenReturn(new MockQueryResultsIterator(new ArrayList<>()));

            String result = contract.QueryAllOps(ctx, "doc1");

            assertThat(result).isEqualTo("[]");
        }

        @Test
        public void returnsSerializedOps() {
            AssetTransfer contract = new AssetTransfer();
            Context ctx = mock(Context.class);
            ChaincodeStub stub = mock(ChaincodeStub.class);
            when(ctx.getStub()).thenReturn(stub);
            when(stub.getStringState("SNAP::doc1")).thenReturn(DOC_EXISTS);

            Operation op = new Operation("op-1", "userA", OperationType.insert, 0, "a", 1000L, 0);
            OperationRecord record = new OperationRecord(op, 1_000_000_000_000L, "tx-abc");
            String recordJson = GENSON.serialize(record);

            List<KeyValue> items = new ArrayList<>();
            items.add(new MockKeyValue("LOG::doc1::00001000000000000000000::tx-abc::op-1", recordJson));
            when(stub.getStateByRange("LOG::doc1::", "LOG::doc1::￿"))
                    .thenReturn(new MockQueryResultsIterator(items));

            String result = contract.QueryAllOps(ctx, "doc1");

            assertThat(result).contains("op-1").contains("userA");
        }
    }

    // -------------------------------------------------------------------------
    // QueryOpsAfter
    // -------------------------------------------------------------------------

    @Nested
    class QueryOpsAfterTransaction {

        @Test
        public void throwsWhenDocumentDoesNotExist() {
            AssetTransfer contract = new AssetTransfer();
            Context ctx = mock(Context.class);
            ChaincodeStub stub = mock(ChaincodeStub.class);
            when(ctx.getStub()).thenReturn(stub);
            when(stub.getStringState("SNAP::doc1")).thenReturn("");

            Throwable thrown = catchThrowable(() -> contract.QueryOpsAfter(ctx, "doc1", ""));

            assertThat(thrown)
                    .isInstanceOf(ChaincodeException.class)
                    .hasMessageContaining("doc1");
        }

        @Test
        public void withEmptyCursorStartsFromBeginning() {
            AssetTransfer contract = new AssetTransfer();
            Context ctx = mock(Context.class);
            ChaincodeStub stub = mock(ChaincodeStub.class);
            when(ctx.getStub()).thenReturn(stub);
            when(stub.getStringState("SNAP::doc1")).thenReturn(DOC_EXISTS);
            when(stub.getStateByRange("LOG::doc1::", "LOG::doc1::￿"))
                    .thenReturn(new MockQueryResultsIterator(new ArrayList<>()));

            String result = contract.QueryOpsAfter(ctx, "doc1", "");

            assertThat(result).isEqualTo("[]");
        }

        @Test
        public void withCursorStartsExclusivelyAfterKey() {
            AssetTransfer contract = new AssetTransfer();
            Context ctx = mock(Context.class);
            ChaincodeStub stub = mock(ChaincodeStub.class);
            when(ctx.getStub()).thenReturn(stub);
            when(stub.getStringState("SNAP::doc1")).thenReturn(DOC_EXISTS);

            String cursor = "LOG::doc1::00001000000000000000000::tx-abc::op-1";
            // The chaincode appends " " to make the range start exclusive.
            when(stub.getStateByRange(cursor + " ", "LOG::doc1::￿"))
                    .thenReturn(new MockQueryResultsIterator(new ArrayList<>()));

            String result = contract.QueryOpsAfter(ctx, "doc1", cursor);

            assertThat(result).isEqualTo("[]");
        }
    }
}
