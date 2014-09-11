package com.jivesoftware.os.miru.query.bitmap;

/**
*
*/
public class CardinalityAndLastSetBit {

    public final long cardinality;
    public final int lastSetBit;

    public CardinalityAndLastSetBit(long cardinality, int lastSetBit) {
        this.cardinality = cardinality;
        this.lastSetBit = lastSetBit;
    }
}
