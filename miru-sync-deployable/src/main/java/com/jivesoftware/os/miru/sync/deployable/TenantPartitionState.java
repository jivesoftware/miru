package com.jivesoftware.os.miru.sync.deployable;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 *
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class TenantPartitionState {

    public final long timeShiftOrderIds;
    public final long timeShiftStartTimestampOrderId;
    public final long timeShiftStopTimestampOrderId;

    @JsonCreator
    public TenantPartitionState(@JsonProperty("timeShiftOrderIds") long timeShiftOrderIds,
        @JsonProperty("timeShiftStartTimestampOrderId") long timeShiftStartTimestampOrderId,
        @JsonProperty("timeShiftStopTimestampOrderId") long timeShiftStopTimestampOrderId) {
        this.timeShiftOrderIds = timeShiftOrderIds;
        this.timeShiftStartTimestampOrderId = timeShiftStartTimestampOrderId;
        this.timeShiftStopTimestampOrderId = timeShiftStopTimestampOrderId;
    }
}
