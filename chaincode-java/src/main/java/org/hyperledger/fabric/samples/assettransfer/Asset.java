/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.hyperledger.fabric.samples.assettransfer;

import org.hyperledger.fabric.contract.annotation.DataType;
import org.hyperledger.fabric.contract.annotation.Property;

import com.owlike.genson.annotation.JsonProperty;

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
}
