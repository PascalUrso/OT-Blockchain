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
    private final long timestamp;

    public Operation(
            @JsonProperty("opId") final String opId,
            @JsonProperty("clientId") final String clientId,
            @JsonProperty("type") final OperationType type,
            @JsonProperty("position") final int position,
            @JsonProperty("value") final String value,
            @JsonProperty("timestamp") final long timestamp,
            @JsonProperty("ack") final long ack) {
        this.opId = opId;
        this.clientId = clientId;
        this.type = type;
        this.position = position;
        this.value = value;
        this.ack = ack;
        this.timestamp = timestamp;
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

    public long getTimestamp() {
        return timestamp;
    }
}
