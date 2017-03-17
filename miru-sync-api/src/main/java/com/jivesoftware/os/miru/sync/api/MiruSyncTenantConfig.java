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

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        MiruSyncTenantConfig that = (MiruSyncTenantConfig) o;

        if (startTimestampMillis != that.startTimestampMillis) {
            return false;
        }
        if (stopTimestampMillis != that.stopTimestampMillis) {
            return false;
        }
        if (timeShiftMillis != that.timeShiftMillis) {
            return false;
        }
        return timeShiftStrategy == that.timeShiftStrategy;

    }

    @Override
    public int hashCode() {
        int result = (int) (startTimestampMillis ^ (startTimestampMillis >>> 32));
        result = 31 * result + (int) (stopTimestampMillis ^ (stopTimestampMillis >>> 32));
        result = 31 * result + (int) (timeShiftMillis ^ (timeShiftMillis >>> 32));
        result = 31 * result + (timeShiftStrategy != null ? timeShiftStrategy.hashCode() : 0);
        return result;
    }
}
