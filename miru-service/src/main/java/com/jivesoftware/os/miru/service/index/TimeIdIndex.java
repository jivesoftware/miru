package com.jivesoftware.os.miru.service.index;

import com.jivesoftware.os.miru.api.MiruPartitionCoord;

/**
 *
 */
public interface TimeIdIndex {

    void lookup(long version, long[] timestamps, int[] ids, long[] monotonics) throws Exception;

    void allocate(MiruPartitionCoord coord,
        long version,
        long[] timestamps,
        int[] ids,
        long[] monotonics,
        int lastIdHint,
        long largestTimestampHint) throws Exception;
}
