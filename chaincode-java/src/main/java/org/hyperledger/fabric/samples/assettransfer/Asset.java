/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.hyperledger.fabric.samples.assettransfer;

import java.util.Objects;

import org.hyperledger.fabric.contract.annotation.DataType;
import org.hyperledger.fabric.contract.annotation.Property;

import com.owlike.genson.annotation.JsonProperty;

/**
 * Snapshot of a collaborative document as stored on the ledger.
 * Holds the document identifier, its current content, and a monotonically
 * increasing version counter (one per committed operation).
 */
@DataType()
public final class Asset {

    @Property()
    private final String docId;

    @Property()
    private final String content;

    @Property()
    private final long version;

    public Asset(@JsonProperty("docId") final String docId,
                 @JsonProperty("content") final String content,
                 @JsonProperty("version") final long version) {
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

    @Override
    public boolean equals(final Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof Asset)) {
            return false;
        }
        Asset other = (Asset) obj;
        return version == other.version
                && Objects.equals(docId, other.docId)
                && Objects.equals(content, other.content);
    }

    @Override
    public int hashCode() {
        return Objects.hash(docId, content, version);
    }

    @Override
    public String toString() {
        return "Asset[docId=" + docId + ", content=" + content + ", version=" + version + "]";
    }
}
