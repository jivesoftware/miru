package com.jivesoftware.os.miru.api.wal;

import com.jivesoftware.os.miru.api.activity.MiruPartitionId;
import java.util.List;

/**
 *
 */
public class MiruActivityWALStatus {

    public MiruPartitionId partitionId;
    public List<WriterCount> counts;
    public List<Integer> begins;
    public List<Integer> ends;

    public MiruActivityWALStatus() {
    }

    public MiruActivityWALStatus(MiruPartitionId partitionId, List<WriterCount> counts, List<Integer> begins, List<Integer> ends) {
        this.partitionId = partitionId;
        this.counts = counts;
        this.begins = begins;
        this.ends = ends;
    }

    public static class WriterCount {

        public int writerId;
        public int count;
        public long clockTimestamp;

        public WriterCount() {
        }

        public WriterCount(int writerId, int count, long clockTimestamp) {
            this.writerId = writerId;
            this.count = count;
            this.clockTimestamp = clockTimestamp;
        }
    }
}
