package com.jivesoftware.os.miru.plugin.bitmap;

/**
*
*/
public class CardinalityAndLastSetBit<BM> {

    public final BM bitmap;
    public final long cardinality;
    public final int lastSetBit;

    public CardinalityAndLastSetBit(BM bitmap, long cardinality, int lastSetBit) {
        this.bitmap = bitmap;
        this.cardinality = cardinality;
        this.lastSetBit = lastSetBit;
    }
}
