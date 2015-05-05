package com.jivesoftware.os.miru.api.wal;

import com.jivesoftware.os.miru.api.activity.MiruPartitionId;
import java.util.List;

/**
 *
 */
public class MiruActivityWALStatus {

    public MiruPartitionId partitionId;
    public long count;
    public List<Integer> begins;
    public List<Integer> ends;

    public MiruActivityWALStatus() {
    }

    public MiruActivityWALStatus(MiruPartitionId partitionId, long count, List<Integer> begins, List<Integer> ends) {
        this.partitionId = partitionId;
        this.count = count;
        this.begins = begins;
        this.ends = ends;
    }
}
