package com.jivesoftware.os.miru.writer.partition;

import com.jivesoftware.os.miru.api.activity.MiruPartitionId;
import java.util.concurrent.atomic.AtomicInteger;

/**
 *
 */
public class MiruPartitionCursor {

    private final MiruPartitionId partitionId;
    private final AtomicInteger index;
    private final int capacity;

    public MiruPartitionCursor(MiruPartitionId partitionId, AtomicInteger index, int capacity) {
        this.partitionId = partitionId;
        this.index = index;
        this.capacity = capacity;
    }

    public MiruPartitionId getPartitionId() {
        return partitionId;
    }

    public boolean hasNext() {
        return index.get() < capacity;
    }

    public int next() {
        int next = index.incrementAndGet();
        if (next > capacity) {
            throw new IndexOutOfBoundsException();
        }
        return next;
    }

    public int last() {
        return index.get();
    }

    @Override
    public String toString() {
        return "MiruPartitionCursor{" + "partitionId=" + partitionId + ", index=" + index + ", capacity=" + capacity + '}';
    }

}
