public final class Operation {
        private final String opId;
        private final String clientId;
        private final OperationType type;
        private final int position;
        private final String value;
        private final long ack;
        private final long timestamp;

        public Operation(final String opId, final String clientId, final OperationType type, final int position,
                        final String value, final long timestamp) {
                this(opId, clientId, type, position, value, timestamp, 0);
        }

        public Operation(final String opId, final String clientId, final OperationType type, final int position,
                        final String value, final long timestamp, final long ack) {
                this.opId = opId;
                this.clientId = clientId;
                this.type = type;
                this.position = position;
                this.value = value;
                this.ack = ack;
                this.timestamp = timestamp;
        }

        public String getOpId() {
                return opId;
        }

        public String getClientId() {
                return clientId;
        }

        public OperationType getType() {
                return type;
        }

        public int getPosition() {
                return position;
        }

        public String getValue() {
                return value;
        }

        public long getAck() {
                return ack;
        }

        public long getTimestamp() {
                return timestamp;
        }
}
