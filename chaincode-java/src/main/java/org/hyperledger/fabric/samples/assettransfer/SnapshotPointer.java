/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.hyperledger.fabric.samples.assettransfer;

import org.hyperledger.fabric.contract.annotation.DataType;
import org.hyperledger.fabric.contract.annotation.Property;

import com.owlike.genson.annotation.JsonProperty;

@DataType()
public final class SnapshotPointer {
    @Property()
    private final String docId;

    @Property()
    private final String snapshotId;

    @Property()
    private final long version;

    @Property()
    private final long timestamp;

    @Property()
    private final int totalChunks;

    @Property()
    private final boolean chunked;

    public SnapshotPointer(
            @JsonProperty("docId") final String docId,
            @JsonProperty("snapshotId") final String snapshotId,
            @JsonProperty("version") final long version,
            @JsonProperty("timestamp") final long timestamp,
            @JsonProperty("totalChunks") final int totalChunks,
            @JsonProperty("chunked") final boolean chunked) {
        this.docId = docId;
        this.snapshotId = snapshotId;
        this.version = version;
        this.timestamp = timestamp;
        this.totalChunks = totalChunks;
        this.chunked = chunked;
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
