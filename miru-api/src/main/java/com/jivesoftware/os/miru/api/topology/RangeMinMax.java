package com.jivesoftware.os.miru.api.topology;

/**
 *
 */
public class RangeMinMax {

    public long clockMin = -1;
    public long clockMax = -1;
    public long orderIdMin = -1;
    public long orderIdMax = -1;

    public RangeMinMax() {
    }

    public void put(long clock, long orderId) {
        if (clockMin == -1 || clock < clockMin) {
            clockMin = clock;
        }
        if (clockMax == -1 || clock > clockMax) {
            clockMax = clock;
        }
        if (orderIdMin == -1 || orderId < orderIdMin) {
            orderIdMin = orderId;
        }
        if (orderIdMax == -1 || orderId > orderIdMax) {
            orderIdMax = orderId;
        }
    }
}
