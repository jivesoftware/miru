package com.jivesoftware.os.miru.sync.deployable;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 *
 */
public class TenantPartitionState {

    public final long timeShiftMillis;
    public final long timeShiftStartTimestampMillis;
    public final long timeShiftStopTimestampMillis;

    @JsonCreator
    public TenantPartitionState(@JsonProperty("timeShiftMillis") long timeShiftMillis,
        @JsonProperty("timeShiftStartTimestampMillis") long timeShiftStartTimestampMillis,
        @JsonProperty("timeShiftStopTimestampMillis") long timeShiftStopTimestampMillis) {
        this.timeShiftMillis = timeShiftMillis;
        this.timeShiftStartTimestampMillis = timeShiftStartTimestampMillis;
        this.timeShiftStopTimestampMillis = timeShiftStopTimestampMillis;
    }
}
