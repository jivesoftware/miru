package com.jivesoftware.os.miru.sync.api;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Created by jonathan.colt on 12/22/16.
 */
public class MiruSyncTenantConfig {

    public final long startTimestampMillis;
    public final long stopTimestampMillis;
    public final long timeShiftMillis;
    public final MiruSyncTimeShiftStrategy timeShiftStrategy;

    @JsonCreator
    public MiruSyncTenantConfig(
        @JsonProperty("startTimestampMillis") long startTimestampMillis,
        @JsonProperty("stopTimestampMillis") long stopTimestampMillis,
        @JsonProperty("timeShiftMillis") long timeShiftMillis,
        @JsonProperty("timeShiftStrategy") MiruSyncTimeShiftStrategy timeShiftStrategy) {

        this.startTimestampMillis = startTimestampMillis;
        this.stopTimestampMillis = stopTimestampMillis;
        this.timeShiftMillis = timeShiftMillis;
        this.timeShiftStrategy = timeShiftStrategy;
    }
}
