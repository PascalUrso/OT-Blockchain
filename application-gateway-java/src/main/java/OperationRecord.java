public final class OperationRecord {
        private final Operation operation;
        private final long committedVersion;
        private final String txId;

        public OperationRecord(final Operation operation, final long committedVersion,
                        final String txId) {
                this.operation = operation;
                this.committedVersion = committedVersion;
                this.txId = txId;
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
