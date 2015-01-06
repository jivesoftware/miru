package com.jivesoftware.os.miru.service.index.memory;

import com.google.common.base.Optional;
import com.jivesoftware.os.filer.map.store.api.KeyValueContext;
import com.jivesoftware.os.filer.map.store.api.KeyValueStore;
import com.jivesoftware.os.filer.map.store.api.KeyValueTransaction;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.plugin.index.MiruTimeIndex;
import com.jivesoftware.os.miru.service.index.BulkExport;
import com.jivesoftware.os.miru.service.index.BulkImport;
import java.io.IOException;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author jonathan
 */
public class MiruInMemoryTimeIndex implements MiruTimeIndex,
    BulkImport<MiruTimeIndex, Void>,
    BulkExport<MiruTimeIndex, Void> {

    //TODO there's a good argument for making this a FileBackMapStore even for the in-memory impl
    //TODO (it's only used for index/repair, so disk paging won't slow reads)

    private long smallestTimestamp = Long.MAX_VALUE;
    private long largestTimestamp = Long.MIN_VALUE;
    private final AtomicInteger id;
    private final KeyValueStore<Long, Integer> timestampToIndex;
    private final Optional<TimeOrderAnomalyStream> timeOrderAnomalyStream;

    private final Object timestampsLock = new Object();
    private long[] timestamps;
    private final int initialCapacity = 32; //TODO configure?

    public MiruInMemoryTimeIndex(Optional<TimeOrderAnomalyStream> timeOrderAnomalyStream,
        KeyValueStore<Long, Integer> timestampToIndex) {
        this.timestamps = new long[initialCapacity];
        this.id = new AtomicInteger(0);
        this.timestampToIndex = timestampToIndex;
        /*
        this.timestampToIndex = new PrimitivesMapStoresBuilder<ChunkFiler>()
            .setMapChunkProvider(mapChunkProvider)
            .setMapChunkFactory(mapChunkFactory)
            .buildLongInt();
        */
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
    public int nextId(long timestamp) throws Exception {
        if (smallestTimestamp == Long.MAX_VALUE) {
            smallestTimestamp = timestamp;
        }
        if (largestTimestamp < timestamp) {
            largestTimestamp = timestamp;
        }

        final int nextId = nextId();
        synchronized (timestampToIndex) {
            timestampToIndex.execute(timestamp, true, new KeyValueTransaction<Integer, Void>() {
                @Override
                public Void commit(KeyValueContext<Integer> keyValueContext) throws IOException {
                    keyValueContext.set(nextId);
                    return null;
                }
            });
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
    public int getExactId(long timestamp) throws Exception {
        Integer index = timestampToIndex.execute(timestamp, false, getExactIdTransaction);
        return index != null ? index : -1;
    }

    @Override
    public boolean contains(long timestamp) throws Exception {
        synchronized (timestampToIndex) {
            return timestampToIndex.execute(timestamp, false, containsTransaction);
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
    public void stream(Stream stream) throws Exception {
        for (int i = 0; i < timestamps.length; i++) {
            if (!stream.stream(new Entry(timestamps[i], i))) {
                break;
            }
        }
    }

    @Override
    public long sizeInMemory() throws Exception {
        return 0;
    }

    @Override
    public long sizeOnDisk() {
        return 0;
    }

    @Override
    public void close() {
    }

    @Override
    public MiruTimeIndex bulkExport(MiruTenantId tenantId, Void callback) throws Exception {
        return this;
    }

    @Override
    public void bulkImport(MiruTenantId tenantId, BulkExport<MiruTimeIndex, Void> export) throws Exception {
        MiruTimeIndex timeIndex = export.bulkExport(tenantId, null);
        timeIndex.stream(new Stream() {
            @Override
            public boolean stream(Entry entry) throws Exception {
                int index = nextId(entry.time);
                if (index != entry.index) {
                    throw new RuntimeException("Import out of order, expected " + index + " but found " + entry.index);
                }
                return true;
            }
        });
    }

    private final KeyValueTransaction<Integer, Integer> getExactIdTransaction = new KeyValueTransaction<Integer, Integer>() {
        @Override
        public Integer commit(KeyValueContext<Integer> keyValueContext) throws IOException {
            return keyValueContext.get();
        }
    };

    private final KeyValueTransaction<Integer, Boolean> containsTransaction = new KeyValueTransaction<Integer, Boolean>() {
        @Override
        public Boolean commit(KeyValueContext<Integer> keyValueContext) throws IOException {
            return keyValueContext.get() != null;
        }
    };

    public static interface TimeOrderAnomalyStream {

        void underflowOfSmallestTimestamp(long delta);

        void underflowOfLargestTimestamp(long delta);

    }
}
