package com.jivesoftware.os.miru.wal.lookup;

/**
 *
 */
public class RangeMinMax {

    private long minClock = -1;
    private long maxClock = -1;
    private long minOrderId = -1;
    private long maxOrderId = -1;

    public void put(long clock, long orderId) {
        if (minClock == -1 || clock < minClock) {
            minClock = clock;
        }
        if (maxClock == -1 || clock > maxClock) {
            maxClock = clock;
        }
        if (minOrderId == -1 || orderId < minOrderId) {
            minOrderId = orderId;
        }
        if (maxOrderId == -1 || orderId > maxOrderId) {
            maxOrderId = orderId;
        }
    }

    public long getMinClock() {
        return minClock;
    }

    public long getMaxClock() {
        return maxClock;
    }

    public long getMinOrderId() {
        return minOrderId;
    }

    public long getMaxOrderId() {
        return maxOrderId;
    }
}
