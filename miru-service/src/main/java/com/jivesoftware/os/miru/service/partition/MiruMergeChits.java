package com.jivesoftware.os.miru.service.partition;

import com.jivesoftware.os.miru.api.MiruPartitionCoord;

/**
 *
 */
public interface MiruMergeChits {
    void refundAll(MiruPartitionCoord coord);

    boolean take(MiruPartitionCoord coord, long count);

    long taken(MiruPartitionCoord coord);

    long remaining();
}
