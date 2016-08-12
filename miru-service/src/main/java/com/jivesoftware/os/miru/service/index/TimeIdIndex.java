package com.jivesoftware.os.miru.service.index;

/**
 *
 */
public interface TimeIdIndex {

    void lookup(long version, long[] timestamps, int[] ids, long[] monotonics) throws Exception;

    void allocate(long version, long[] timestamps, int[] ids, long[] monotonics, int lastIdHint, long largestTimestampHint) throws Exception;
}
