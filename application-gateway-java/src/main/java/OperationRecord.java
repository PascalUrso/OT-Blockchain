public final class OperationRecord {
        private final String docId;
        private final Operation operation;
        private final long committedVersion;
        private final String txId;

        public OperationRecord(final String docId, final Operation operation, final long committedVersion,
                        final String txId) {
                this.docId = docId;
                this.operation = operation;
                this.committedVersion = committedVersion;
                this.txId = txId;
        }

        public String getDocId() {
                return docId;
        }

        public Operation getOperation() {
                return operation;
        }

        public long getCommittedVersion() {
                return committedVersion;
        }

        public String getTxId() {
                return txId;
        }
}
