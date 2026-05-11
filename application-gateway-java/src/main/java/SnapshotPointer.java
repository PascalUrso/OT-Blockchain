public final class SnapshotPointer {
    private final String docId;
    private final String snapshotId;
    private final long version;
    private final long timestamp;
    private final int totalChunks;
    private final boolean chunked;

    public SnapshotPointer(final String docId,
            final String snapshotId,
            final long version,
            final long timestamp,
            final int totalChunks) {
        this.docId = docId;
        this.snapshotId = snapshotId;
        this.version = version;
        this.timestamp = timestamp;
        this.totalChunks = totalChunks;
        this.chunked = true;
    }

    public String getDocId() {
        return docId;
    }

    public String getSnapshotId() {
        return snapshotId;
    }

    public long getVersion() {
        return version;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public int getTotalChunks() {
        return totalChunks;
    }

    public boolean isChunked() {
        return chunked;
    }
}
