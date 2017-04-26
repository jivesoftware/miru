package com.jivesoftware.os.miru.sync.api;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Created by jonathan.colt on 12/22/16.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class MiruSyncTenantConfig {

    public final long startTimestampMillis;
    public final long stopTimestampMillis;
    public final long timeShiftMillis;
    public final long timeShiftStartTimestampMillis;
    public final long timeShiftStopTimestampMillis;
    public final MiruSyncTimeShiftStrategy timeShiftStrategy;
    public final boolean closed;

    @JsonCreator
    public MiruSyncTenantConfig(
        @JsonProperty("startTimestampMillis") long startTimestampMillis,
        @JsonProperty("stopTimestampMillis") long stopTimestampMillis,
        @JsonProperty("timeShiftMillis") long timeShiftMillis,
        @JsonProperty("timeShiftStartTimestampMillis") long timeShiftStartTimestampMillis,
        @JsonProperty("timeShiftStopTimestampMillis") long timeShiftStopTimestampMillis,
        @JsonProperty("timeShiftStrategy") MiruSyncTimeShiftStrategy timeShiftStrategy,
        @JsonProperty("closed") boolean closed) {

        this.startTimestampMillis = startTimestampMillis;
        this.stopTimestampMillis = stopTimestampMillis;
        this.timeShiftMillis = timeShiftMillis;
        this.timeShiftStartTimestampMillis = timeShiftStartTimestampMillis;
        this.timeShiftStopTimestampMillis = timeShiftStopTimestampMillis;
        this.timeShiftStrategy = timeShiftStrategy;
        this.closed = closed;
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
        if (timeShiftStartTimestampMillis != that.timeShiftStartTimestampMillis) {
            return false;
        }
        if (timeShiftStopTimestampMillis != that.timeShiftStopTimestampMillis) {
            return false;
        }
        if (closed != that.closed) {
            return false;
        }
        return timeShiftStrategy == that.timeShiftStrategy;

    }

    @Override
    public int hashCode() {
        int result = (int) (startTimestampMillis ^ (startTimestampMillis >>> 32));
        result = 31 * result + (int) (stopTimestampMillis ^ (stopTimestampMillis >>> 32));
        result = 31 * result + (int) (timeShiftMillis ^ (timeShiftMillis >>> 32));
        result = 31 * result + (int) (timeShiftStartTimestampMillis ^ (timeShiftStartTimestampMillis >>> 32));
        result = 31 * result + (int) (timeShiftStopTimestampMillis ^ (timeShiftStopTimestampMillis >>> 32));
        result = 31 * result + (timeShiftStrategy != null ? timeShiftStrategy.hashCode() : 0);
        result = 31 * result + (closed ? 1 : 0);
        return result;
    }
}
