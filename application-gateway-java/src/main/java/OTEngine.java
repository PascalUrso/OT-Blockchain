public final class OTEngine {
    private OTEngine() {
    }

    public static Operation transform(final Operation op1, final Operation op2) {
        if (isNoop(op1)) {
            return op1;
        }
        if (isNoop(op2)) {
            return op1;
        }

        int p1 = op1.getPosition();
        int p2 = op2.getPosition();
        int len2 = span(op2);

        if (op1.getType() == OperationType.insert && op2.getType() == OperationType.insert) {
            if (p1 > p2 || (p1 == p2 && op1.getClientId().compareTo(op2.getClientId()) > 0)) {
                return withPosition(op1, p1 + len2);
            }
            return op1;
        }

        if (op1.getType() == OperationType.insert && op2.getType() == OperationType.delete) {
            if (p1 > p2) {
                return withPosition(op1, Math.max(p2, p1 - len2));
            }
            return op1;
        }

        if (op1.getType() == OperationType.delete && op2.getType() == OperationType.insert) {
            if (p1 >= p2) {
                return withPosition(op1, p1 + len2);
            }
            return op1;
        }

        if (op1.getType() == OperationType.delete && op2.getType() == OperationType.delete) {
            if (p1 == p2) {
                return toNoop(op1);
            }
            if (p1 > p2) {
                return withPosition(op1, Math.max(p2, p1 - len2));
            }
            return op1;
        }

        if (op1.getType() == OperationType.update && op2.getType() == OperationType.insert) {
            if (p1 >= p2) {
                return withPosition(op1, p1 + len2);
            }
            return op1;
        }

        if (op1.getType() == OperationType.update && op2.getType() == OperationType.delete) {
            if (p1 == p2) {
                return toNoop(op1);
            }
            if (p1 > p2) {
                return withPosition(op1, Math.max(p2, p1 - len2));
            }
            return op1;
        }

        if (op1.getType() == OperationType.delete && op2.getType() == OperationType.update) {
            return op1;
        }

        if (op1.getType() == OperationType.insert && op2.getType() == OperationType.update) {
            return op1;
        }

        if (op1.getType() == OperationType.update && op2.getType() == OperationType.update) {
            return op1;
        }

        // if (op2.getType() == OperationType.insert && p1 >= p2) {
        //     return withPosition(op1, p1 + len2);
        // }
        // if (op2.getType() == OperationType.delete && p1 > p2) {
        //     return withPosition(op1, Math.max(p2, p1 - len2));
        // }
        return op1;
    }

    public static String apply(final String content, final Operation op) {
        if (isNoop(op)) {
            return content;
        }

        int pos = op.getPosition();
        String value = op.getValue() == null ? "" : op.getValue();

        if (pos < 0 || pos > content.length()) {
            throw new IllegalArgumentException("position out of range: " + pos);
        }

        switch (op.getType()) {
            case insert:
                return content.substring(0, pos) + value + content.substring(pos);
            case delete:
                int deleteSpan = value.isEmpty() ? 1 : value.length();
                if (pos + deleteSpan > content.length()) {
                    throw new IllegalArgumentException("delete out of range");
                }
                return content.substring(0, pos) + content.substring(pos + deleteSpan);
            case update:
                int updateSpan = value.isEmpty() ? 1 : value.length();
                if (pos + updateSpan > content.length()) {
                    throw new IllegalArgumentException("update out of range");
                }
                return content.substring(0, pos) + value + content.substring(pos + updateSpan);
            default:
                throw new IllegalArgumentException("unsupported type: " + op.getType());
        }
    }

    private static int span(final Operation op) {
        if (isNoop(op)) {
            return 0;
        }

        if (op.getType() == OperationType.insert) {
            return op.getValue() == null ? 0 : op.getValue().length();
        }
        if (op.getValue() == null || op.getValue().isEmpty()) {
            return 1;
        }
        return op.getValue().length();
    }

    private static Operation withPosition(final Operation op, final int pos) {
        return new Operation(op.getOpId(), op.getClientId(), op.getType(), pos, op.getValue(),
            op.getTimestamp(), op.getAck());
    }

    private static Operation toNoop(final Operation op) {
        return withPosition(op, -1);
    }

    private static boolean isNoop(final Operation op) {
        return op.getPosition() < 0;
    }
}
