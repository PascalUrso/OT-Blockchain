/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.hyperledger.fabric.samples.assettransfer;

import org.hyperledger.fabric.contract.annotation.DataType;
import org.hyperledger.fabric.contract.annotation.Property;

import com.owlike.genson.annotation.JsonProperty;

@DataType()
public final class Operation {
    @Property()
    private final String opId;

    @Property()
    private final String clientId;

    @Property()
    private final OperationType type;

    @Property()
    private final int position;

    @Property()
    private final String value;

    @Property()
    private final long ack;

    @Property()
    private final long clientSeq;

    @Property()
    private final long timestamp;

    @Property()
    private final long lastEventBlock;

    @Property()
    private final int lastEventTxIndex;

    @Property()
    private final int lastEventActionIndex;


    public Operation(
            @JsonProperty("opId") final String opId,
            @JsonProperty("clientId") final String clientId,
            @JsonProperty("type") final OperationType type,
            @JsonProperty("position") final int position,
            @JsonProperty("value") final String value,
            @JsonProperty("timestamp") final long timestamp,
            @JsonProperty("ack") final long ack,
            @JsonProperty("clientSeq") final long clientSeq,
            @JsonProperty("lastEventBlock") final long lastEventBlock,
            @JsonProperty("lastEventTxIndex") final int lastEventTxIndex,
            @JsonProperty("lastEventActionIndex") final int lastEventActionIndex) {
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
