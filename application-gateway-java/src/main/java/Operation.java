        public final class Operation {
                private final String opId;
                private final String clientId;
                private final OperationType type;
                private final int position;
                private final String value;
                private final long ack;
                private final long clientSeq;
                private final long timestamp;
                private final long lastEventBlock;
                private final int lastEventTxIndex;
                private final int lastEventActionIndex;

                public Operation(final String opId, final String clientId, final OperationType type, final int position,
                                final String value, final long timestamp) {
                                        this(opId, clientId, type, position, value, timestamp, 0, -1, -1, -1, -1);
                }

                public Operation(final String opId, final String clientId, final OperationType type, final int position,
                                final String value, final long timestamp, final long ack) {
                                        this(opId, clientId, type, position, value, timestamp, ack, -1, -1, -1, -1);
                }

                public Operation(final String opId, final String clientId, final OperationType type, final int position,
                                                final String value, final long timestamp, final long ack, final long clientSeq,
                                                final long lastEventBlock, final int lastEventTxIndex, final int lastEventActionIndex) {
                        this.opId = opId;
                        this.clientId = clientId;
                        this.type = type;
                        this.position = position;
                        this.value = value;
                        this.ack = ack;
                        this.clientSeq = clientSeq;
                        this.timestamp = timestamp;
                        this.lastEventBlock = lastEventBlock;
                        this.lastEventTxIndex = lastEventTxIndex;
                        this.lastEventActionIndex = lastEventActionIndex;
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

                public long getClientSeq() {
                        return clientSeq;
                }

                public long getTimestamp() {
                        return timestamp;
                }

                public long getLastEventBlock() {
                        return lastEventBlock;
                }

                public int getLastEventTxIndex() {
                        return lastEventTxIndex;
                }

                public int getLastEventActionIndex() {
                        return lastEventActionIndex;
                }
        }
