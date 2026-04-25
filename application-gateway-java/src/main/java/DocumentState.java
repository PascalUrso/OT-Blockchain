public final class DocumentState {
	private final String docId;
	private final String content;
	private final long version;

	public DocumentState(final String docId, final String content, final long version) {
		this.docId = docId;
		this.content = content;
		this.version = version;
	}

	public String getDocId() {
		return docId;
	}

	public String getContent() {
		return content;
	}

	public long getVersion() {
		return version;
	}
}
