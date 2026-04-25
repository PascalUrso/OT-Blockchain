/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.hyperledger.fabric.samples.assettransfer;

import org.hyperledger.fabric.contract.annotation.DataType;
import org.hyperledger.fabric.contract.annotation.Property;

import com.owlike.genson.annotation.JsonProperty;

@DataType()
public final class OperationRecord {
    @Property()
    private final Operation operation;

    @Property()
    private final long committedVersion;

    @Property()
    private final String txId;

    public OperationRecord(
            @JsonProperty("operation") final Operation operation,
            @JsonProperty("committedVersion") final long committedVersion,
            @JsonProperty("txId") final String txId) {
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
