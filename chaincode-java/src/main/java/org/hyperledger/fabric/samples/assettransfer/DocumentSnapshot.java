/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.hyperledger.fabric.samples.assettransfer;

import java.util.List;
import java.util.Map;
import java.util.Set;

import org.hyperledger.fabric.contract.annotation.DataType;
import org.hyperledger.fabric.contract.annotation.Property;

import com.owlike.genson.annotation.JsonProperty;

@DataType()
public final class DocumentSnapshot {
    @Property()
    private final String snapshotId;

    @Property()
    private final String docId;

    @Property()
    private final long version;

    @Property()
    private final long timestamp;

    @Property()
    private final long lastBlockNumber;

    @Property()
    private final String lastEventTxId;

    @Property()
    private final String lastEventOpId;

    @Property()
    private final String committedView;

    @Property()
    private final Map<String, List<Operation>> clientBuffers;

    @Property()
    private final Set<String> knownClients;

    public DocumentSnapshot(
            @JsonProperty("snapshotId") final String snapshotId,
            @JsonProperty("docId") final String docId,
            @JsonProperty("version") final long version,
            @JsonProperty("timestamp") final long timestamp,
            @JsonProperty("lastBlockNumber") final long lastBlockNumber,
            @JsonProperty("lastEventTxId") final String lastEventTxId,
            @JsonProperty("lastEventOpId") final String lastEventOpId,
            @JsonProperty("committedView") final String committedView,
            @JsonProperty("clientBuffers") final Map<String, List<Operation>> clientBuffers,
            @JsonProperty("knownClients") final Set<String> knownClients) {
        this.snapshotId = snapshotId;
        this.docId = docId;
        this.version = version;
        this.timestamp = timestamp;
        this.lastBlockNumber = lastBlockNumber;
        this.lastEventTxId = lastEventTxId;
        this.lastEventOpId = lastEventOpId;
        this.committedView = committedView;
        this.clientBuffers = clientBuffers;
        this.knownClients = knownClients;
    }

    public String getSnapshotId() {
        return snapshotId;
    }

    public String getDocId() {
        return docId;
    }


    public long getVersion() {
        return version;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public long getLastBlockNumber() {
        return lastBlockNumber;
    }

    public String getLastEventTxId() {
        return lastEventTxId;
    }

    public String getLastEventOpId() {
        return lastEventOpId;
    }

    public String getCommittedView() {
        return committedView;
    }


    public Map<String, List<Operation>> getClientBuffers() {
        return clientBuffers;
    }

    public Set<String> getKnownClients() {
        return knownClients;
    }
}
