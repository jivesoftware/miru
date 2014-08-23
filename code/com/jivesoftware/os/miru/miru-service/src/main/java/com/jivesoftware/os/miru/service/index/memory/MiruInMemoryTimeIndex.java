package com.jivesoftware.os.miru.service.index.memory;

import com.google.common.base.Optional;
import com.jivesoftware.os.miru.service.index.BulkExport;
import com.jivesoftware.os.miru.service.index.BulkImport;
import com.jivesoftware.os.miru.service.index.MiruTimeIndex;
import gnu.trove.impl.Constants;
import gnu.trove.iterator.TLongIntIterator;
import gnu.trove.map.hash.TLongIntHashMap;

import java.util.Arrays;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author jonathan
 */
public class MiruInMemoryTimeIndex implements MiruTimeIndex, BulkImport<long[]>, BulkExport<MiruTimeIndex> {

    //TODO there's a good argument for making this a FileBackMapStore even for the in-memory impl
    //TODO (it's only used for index/repair, so disk paging won't slow reads)

    private final TLongIntHashMap timestampToIndex = new TLongIntHashMap(Constants.DEFAULT_CAPACITY, Constants.DEFAULT_LOAD_FACTOR, -1, -1);

    private long smallestTimestamp = Long.MAX_VALUE;
    private long largestTimestamp = Long.MIN_VALUE;
    private final AtomicInteger id;
    private final Optional<TimeOrderAnomalyStream> timeOrderAnomalyStream;

    private final Object timestampsLock = new Object();
    private long[] timestamps;
    private final int initialCapacity = 32; //TODO configure?

    public MiruInMemoryTimeIndex(Optional<TimeOrderAnomalyStream> timeOrderAnomalyStream) {
        this.timestamps = new long[initialCapacity];
        this.id = new AtomicInteger(0);
        this.timeOrderAnomalyStream = timeOrderAnomalyStream;
    }

    private int nextId() {
        return id.getAndIncrement();
    }

    @Override
    public int lastId() {
        return id.get() - 1;
    }

    @Override
    public long getSmallestTimestamp() {
        return smallestTimestamp;
    }

    @Override
    public long getLargestTimestamp() {
        return largestTimestamp;
    }

    @Override
    public long getTimestamp(int id) {
        long[] timestamps = this.timestamps; // stable reference
        return id < timestamps.length ? timestamps[id] : 0l;
    }

    @Override
    public int nextId(long timestamp) {
        if (smallestTimestamp == Long.MAX_VALUE) {
            smallestTimestamp = timestamp;
        }
        if (largestTimestamp < timestamp) {
            largestTimestamp = timestamp;
        }

        int nextId = nextId();
        synchronized (timestampToIndex) {
            timestampToIndex.put(timestamp, nextId);
        }
        if (timestamp < smallestTimestamp) {
            if (timeOrderAnomalyStream.isPresent()) {
                timeOrderAnomalyStream.get().underflowOfSmallestTimestamp(smallestTimestamp - timestamp);
            }
        } else if (timestamp != largestTimestamp) {
            if (timeOrderAnomalyStream.isPresent()) {
                timeOrderAnomalyStream.get().underflowOfLargestTimestamp(largestTimestamp - timestamp);
            }
        }

        growAndSet(nextId, largestTimestamp);
        return nextId;
    }

    private void growAndSet(int id, long value) {
        synchronized (timestampsLock) {
            if (id >= timestamps.length) {
                int newLength = timestamps.length * 2;
                while (newLength <= id) {
                    newLength *= 2;
                }

                long[] newTimestamps = new long[newLength];
                System.arraycopy(timestamps, 0, newTimestamps, 0, timestamps.length);
                timestamps = newTimestamps;
            }
            timestamps[id] = value;
        }
    }

    /*
     Returns true index of activityTimestamp or where it would have been.
     */
    @Override
    public int getClosestId(long timestamp) {
        long[] timestamps = this.timestamps; // stable reference
        int index = Arrays.binarySearch(timestamps, 0, id.get(), timestamp);
        if (index >= 0) {
            return index;
        } else {
            return -(index + 1);
        }
    }

    @Override
    public int getExactId(long timestamp) {
        Integer index = timestampToIndex.get(timestamp);
        return index != null ? index : -1;
    }

    @Override
    public boolean contains(long timestamp) {
        synchronized (timestampToIndex) {
            return timestampToIndex.containsKey(timestamp);
        }
    }

    @Override
    public int smallestExclusiveTimestampIndex(long timestamp) {
        long[] timestamps = this.timestamps; // stable reference
        int index = getClosestId(timestamp);
        if (index == 0) {
            return 0;
        }
        int lastId = lastId();
        if (index > lastId) {
            return lastId + 1;
        }
        while (index <= lastId && timestamps[index] <= timestamp) {
            index++;
        }
        return index;
    }

    @Override
    public int largestInclusiveTimestampIndex(long timestamp) {
        long[] timestamps = this.timestamps; // stable reference
        int capacity = timestamps.length;
        int index = getClosestId(timestamp);
        if (index == capacity) {
            return capacity - 1;
        }
        int lastId = lastId();
        if (index > lastId) {
            return lastId;
        }
        while (index <= lastId && timestamps[index] <= timestamp) {
            index++;
        }
        return index - 1;
    }

    @Override
    public Iterable<Entry> getEntries() {
        return new Iterable<Entry>() {
            @Override
            public Iterator<Entry> iterator() {
                return new TLongIntIteratorAdapter(timestampToIndex.iterator());
            }
        };
    }

    private static final class TLongIntIteratorAdapter implements Iterator<Entry> {

        private final TLongIntIterator iter;

        private TLongIntIteratorAdapter(TLongIntIterator iter) {
            this.iter = iter;
        }

        @Override
        public boolean hasNext() {
            return iter.hasNext();
        }

        @Override
        public Entry next() {
            long key = iter.key();
            int value = iter.value();
            iter.advance();
            return new Entry(key, value);
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }
    }

    @Override
    public long sizeInMemory() {
        return timestamps.length * 8 // pointer
                + timestampToIndex.size() * 12; // long + int
    }

    @Override
    public long sizeOnDisk() {
        return 0;
    }

    @Override
    public void close() {
    }

    @Override
    public MiruTimeIndex bulkExport() throws Exception {
        return this;
    }

    @Override
    public void bulkImport(BulkExport<long[]> importItems) throws Exception {
        long[] importArray = importItems.bulkExport();
        for (long timestamp : importArray) {
            nextId(timestamp);
        }
    }

    public static interface TimeOrderAnomalyStream {

        void underflowOfSmallestTimestamp(long delta);

        void underflowOfLargestTimestamp(long delta);

    }
}
