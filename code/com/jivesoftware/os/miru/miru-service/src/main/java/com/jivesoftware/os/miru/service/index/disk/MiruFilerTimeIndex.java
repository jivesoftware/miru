package com.jivesoftware.os.miru.service.index.disk;

import com.google.common.base.Optional;
import com.jivesoftware.os.filer.io.Filer;
import com.jivesoftware.os.filer.io.FilerIO;
import com.jivesoftware.os.filer.io.FilerTransaction;
import com.jivesoftware.os.filer.map.store.api.KeyValueContext;
import com.jivesoftware.os.filer.map.store.api.KeyValueStore;
import com.jivesoftware.os.filer.map.store.api.KeyValueTransaction;
import com.jivesoftware.os.miru.plugin.index.MiruTimeIndex;
import com.jivesoftware.os.miru.service.index.MiruFilerProvider;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author jonathan
 */
public class MiruFilerTimeIndex implements MiruTimeIndex {

    private static final MetricLogger log = MetricLoggerFactory.getLogger();

    private static final int HEADER_SIZE_IN_BYTES = 4 + 8 + 8 + 4 + 2 + 2;
    private static final short LEVELS = 3; // TODO - Config or allow it to be passed in?
    private static final short SEGMENTS = 10; // TODO - Config or allow it to be passed in?

    private static final int timestampSize = 8;
    private static final int searchIndexKeySize = 8;
    private static final int searchIndexValueSize = 4;

    private final AtomicInteger id = new AtomicInteger(-1);
    private long smallestTimestamp;
    private long largestTimestamp;
    private int timestampsLength;
    private int searchIndexLevels;
    private int searchIndexSegments;
    private int searchIndexSizeInBytes;

    private final MiruFilerProvider filerProvider;
    private final Optional<TimeOrderAnomalyStream> timeOrderAnomalyStream;
    private final KeyValueStore<Long, Integer> timestampToIndex;

    public MiruFilerTimeIndex(Optional<TimeOrderAnomalyStream> timeOrderAnomalyStream,
        MiruFilerProvider filerProvider,
        KeyValueStore<Long, Integer> timestampToIndex)
        throws IOException {

        this.filerProvider = filerProvider;
        this.timeOrderAnomalyStream = timeOrderAnomalyStream;
        this.timestampToIndex = timestampToIndex;

        init();
    }

    private void init() throws IOException {
        final AtomicBoolean initialized = new AtomicBoolean(false);

        //TODO consider using a custom CreateFiler in the KeyValueStore to handle the uninitialized case
        filerProvider.execute(-1, new FilerTransaction<Filer, Void>() {
            @Override
            public Void commit(Object lock, Filer filer) throws IOException {
                if (filer != null) {
                    synchronized (lock) {
                        filer.seek(0);
                        MiruFilerTimeIndex.this.id.set(FilerIO.readInt(filer, "lastId"));
                        MiruFilerTimeIndex.this.smallestTimestamp = FilerIO.readLong(filer, "smallestTimestamp");
                        MiruFilerTimeIndex.this.largestTimestamp = FilerIO.readLong(filer, "largestTimestamp");
                        MiruFilerTimeIndex.this.timestampsLength = FilerIO.readInt(filer, "timestampsLength");
                        MiruFilerTimeIndex.this.searchIndexLevels = FilerIO.readShort(filer, "searchIndexLevels");
                        MiruFilerTimeIndex.this.searchIndexSegments = FilerIO.readShort(filer, "searchIndexSegments");
                        MiruFilerTimeIndex.this.searchIndexSizeInBytes = segmentWidth(searchIndexLevels, searchIndexSegments);
                        initialized.set(true);
                    }
                }
                return null;
            }
        });

        if (!initialized.get()) {
            this.id.set(-1);
            this.smallestTimestamp = Long.MAX_VALUE;
            this.largestTimestamp = Long.MIN_VALUE;
            this.timestampsLength = 0;
            this.searchIndexLevels = LEVELS;
            this.searchIndexSegments = SEGMENTS;
            this.searchIndexSizeInBytes = segmentWidth(LEVELS, SEGMENTS);

            filerProvider.execute(HEADER_SIZE_IN_BYTES, new FilerTransaction<Filer, Void>() {
                @Override
                public Void commit(Object o, Filer filer) throws IOException {

                    filer.seek(0);
                    FilerIO.writeInt(filer, MiruFilerTimeIndex.this.id.get(), "lastId");
                    FilerIO.writeLong(filer, MiruFilerTimeIndex.this.smallestTimestamp, "smallestTimestamp");
                    FilerIO.writeLong(filer, MiruFilerTimeIndex.this.largestTimestamp, "largestTimestamp");
                    FilerIO.writeInt(filer, MiruFilerTimeIndex.this.timestampsLength, "timestampsLength");
                    FilerIO.writeShort(filer, MiruFilerTimeIndex.this.searchIndexLevels, "searchIndexLevels");
                    FilerIO.writeShort(filer, MiruFilerTimeIndex.this.searchIndexSegments, "searchIndexSegments");

                    return null;
                }
            });
        }
    }

    /**
     * Example: Levels=4, Segments=4, Timestamps=[0, 2048] Note: Bracketed keys '{}' are long timestamps. Asterisked values '*' are integer ids.
     * <pre>
     * { 0, 512, 1024, 1536 }           // remainingLevels = 4, bytes = s*8 + s*(s*8 + s*(s*8 + s*(s*8 + s*4)))
     *      0: { 0, 128, 256, 384 }     // remainingLevels = 3, bytes = s*8 + s*(s*8 + s*(s*8 + s*4))
     *          0: { 0, 32, 64, 96 }    // remainingLevels = 2, bytes = s*8 + s*(s*8 + s*4)
     *              0: { 0, 8, 16, 24 } // remainingLevels = 1, bytes = s*8 + s*4
     *                  0:  *10_001     // bytes = 4
     *                  8:  *10_003     // bytes = 4
     *                  16: *10_009     // bytes = 4
     *                  24: *10_014     // bytes = 4
     *              32: { 32, 40,  48,  56  }
     *              64: { 64, 72,  80,  88  }
     *              96: { 96, 104, 112, 120 }
     *          128: { 128, 160, 192, 224 }
     *          256: { 256, 288, 320, 352 }
     *          384: { 384, 416, 448, 480 }
     *      512:  { 512,  640,  768,  896  }
     *      1024: { 1024, 1152, 1280, 1408 }
     *      1536: { 1536, 1664, 1792, 1920 }
     * </pre>
     */
    private static void segmentForSearch(Filer filer, long searchIndexSizeInBytes, int remainingLevels, int segments, long fp, int smallestId, int largestId)
        throws IOException {
        final int delta = largestId - smallestId;

        // first select the ids for this section
        int[] segmentAtIds = new int[segments];
        for (int i = 0; i < segments; i++) {
            segmentAtIds[i] = smallestId + delta * i / segments;
        }

        // next get the timestamp for each selected id
        long[] timestampAtIds = new long[segments];
        for (int i = 0; i < segments; i++) {
            filer.seek(HEADER_SIZE_IN_BYTES + searchIndexSizeInBytes + segmentAtIds[i] * timestampSize);

            timestampAtIds[i] = FilerIO.readLong(filer, "ts");
        }

        // now write the timestamps for this section
        filer.seek(fp);
        for (int i = 0; i < segments; i++) {
            FilerIO.writeLong(filer, timestampAtIds[i], "ts");
        }

        // finally write the table for the next level, or the ids if this is the final level
        long subSegmentsFp = fp + segments * searchIndexKeySize;
        filer.seek(subSegmentsFp);

        remainingLevels--;
        if (remainingLevels == 0) {
            // no more levels, so write id values
            for (int i = 0; i < segments; i++) {
                FilerIO.writeInt(filer, segmentAtIds[i], "id");
            }
        } else {
            // write subsections for next level
            int subSegmentWidth = segmentWidth(remainingLevels, segments);
            for (int i = 0; i < segments; i++) {
                int segmentSmallestId = segmentAtIds[i];
                int segmentLargestId = (i < segments - 1) ? segmentAtIds[i + 1] : largestId;
                segmentForSearch(filer, searchIndexSizeInBytes, remainingLevels, segments, subSegmentsFp + i * subSegmentWidth,
                    segmentSmallestId, segmentLargestId);
            }
        }
    }

    private static int segmentWidth(int remainingLevels, int segments) {
        int offset = segments * (searchIndexKeySize + searchIndexValueSize); // size of index and id values in lowest level
        for (int i = 0; i < remainingLevels - 1; i++) {
            offset = segments * (searchIndexKeySize + offset); // size of index and sub-levels (cascading)
        }
        return offset;
    }

    @Override
    public int lastId() {
        return id.get();
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
    public long getTimestamp(final int id) {
        if (id >= timestampsLength) {
            return 0l;
        }
        try {
            return filerProvider.execute(-1, new FilerTransaction<Filer, Long>() {
                @Override
                public Long commit(Object lock, Filer filer) throws IOException {
                    synchronized (lock) {
                        filer.seek(HEADER_SIZE_IN_BYTES + searchIndexSizeInBytes + id * timestampSize);
                        return FilerIO.readLong(filer, "ts");
                    }
                }
            });
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private long readTimestamp(Object lock, Filer filer, int id) throws IOException {
        synchronized (lock) {
            filer.seek(HEADER_SIZE_IN_BYTES + searchIndexSizeInBytes + id * timestampSize);
            return FilerIO.readLong(filer, "ts");
        }
    }

    @Override
    public int[] nextId(final long... timestamps) throws IOException {
        final int[] nextIds = new int[timestamps.length];

        try {
            for (int i = 0; i < timestamps.length; i++) {
                long timestamp = timestamps[i];
                nextIds[i] = id.incrementAndGet();

                if (smallestTimestamp == Long.MAX_VALUE) {
                    smallestTimestamp = timestamp;
                }
                if (largestTimestamp < timestamp) {
                    largestTimestamp = timestamp;
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
            }

            final int lastId = nextIds[nextIds.length - 1];
            final long longs = (lastId + 1) * 8;

            filerProvider.execute(HEADER_SIZE_IN_BYTES + searchIndexSizeInBytes + longs, new FilerTransaction<Filer, Void>() {
                @Override
                public Void commit(Object lock, Filer filer) throws IOException {
                    synchronized (lock) {
                        filer.seek(0);
                        FilerIO.writeInt(filer, lastId, "int");
                        FilerIO.writeLong(filer, smallestTimestamp, "smallestTimestamp");
                        FilerIO.writeLong(filer, largestTimestamp, "largestTimestamp");
                        timestampsLength = lastId + 1;
                        FilerIO.writeInt(filer, timestampsLength, "timestampsLength");

                        filer.seek(HEADER_SIZE_IN_BYTES + searchIndexSizeInBytes + (nextIds[0] * 8));
                        for (long timestamp : timestamps) {
                            FilerIO.writeLong(filer, timestamp, "long");
                        }

                        segmentForSearch(filer, searchIndexSizeInBytes, searchIndexLevels, searchIndexSegments, HEADER_SIZE_IN_BYTES, 0, lastId + 1);

                    }
                    return null;
                }
            });
            for (int i = 0; i < nextIds.length; i++) {
                final int nextId = nextIds[i];
                // TODO would be nice to batch here :)
                timestampToIndex.execute(timestamps[i], true, new KeyValueTransaction<Integer, Void>() {
                    @Override
                    public Void commit(KeyValueContext<Integer> keyValueContext) throws IOException {
                        keyValueContext.set(nextId);
                        return null;
                    }
                });
            }

            return nextIds;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

    }

    /*
     Returns true index of activityTimestamp or where it would have been.
     */
    @Override
    public int getClosestId(final long timestamp) {
        try {
            return filerProvider.execute(-1, new FilerTransaction<Filer, Integer>() {
                @Override
                public Integer commit(Object lock, Filer filer) throws IOException {
                    if (filer != null) {
                        return readClosestId(lock, filer, timestamp);
                    }
                    return -1;
                }
            });
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private int readClosestId(Object lock, Filer filer, long timestamp) throws IOException {
        long fp = HEADER_SIZE_IN_BYTES;
        if (id.get() < 0) {
            return -1;
        }
        synchronized (lock) {
            filer.seek(fp);
            for (int remainingLevels = searchIndexLevels - 1; remainingLevels >= 0; remainingLevels--) {
                int segmentIndex = 0;
                for (; segmentIndex < searchIndexSegments; segmentIndex++) {
                    long segmentTimestamp = FilerIO.readLong(filer, "ts");
                    if (segmentTimestamp > timestamp) {
                        break;
                    }
                }
                segmentIndex--; // last index whose timestamp was less than the requested timestamp
                if (segmentIndex < 0) {
                    segmentIndex = 0; // underflow, just keep tracking towards the smallest segment
                }
                fp += searchIndexSegments * searchIndexKeySize;
                if (remainingLevels == 0) {
                    fp += segmentIndex * searchIndexValueSize;
                } else {
                    fp += segmentIndex * segmentWidth(remainingLevels, searchIndexSegments);
                }
                filer.seek(fp);
            }

            int id = FilerIO.readInt(filer, "id");
            long lastId = lastId();
            while (id <= lastId && readTimestamp(lock, filer, id) < timestamp) {
                id++;
            }

            return id;
        }
    }

    private final KeyValueTransaction<Integer, Integer> exactIdTransaction = new KeyValueTransaction<Integer, Integer>() {
        @Override
        public Integer commit(KeyValueContext<Integer> keyValueContext) throws IOException {
            return keyValueContext.get();
        }
    };

    @Override
    public int getExactId(long timestamp) throws Exception {
        Integer id = timestampToIndex.execute(timestamp, false, exactIdTransaction);
        return id != null ? id : -1;
    }

    @Override
    public boolean[] contains(List<Long> timestamp) throws Exception {
        return timestampToIndex.contains(timestamp);
    }

    @Override
    public int smallestExclusiveTimestampIndex(final long timestamp) {
        try {
            return filerProvider.execute(-1, new FilerTransaction<Filer, Integer>() {
                @Override
                public Integer commit(Object lock, Filer filer) throws IOException {
                    if (filer != null) {
                        int index = readClosestId(lock, filer, timestamp);
                        if (index <= 0) {
                            return 0;
                        }
                        int lastId = lastId();
                        if (index > lastId) {
                            return lastId + 1;
                        }
                        while (index <= lastId && readTimestamp(lock, filer, index) <= timestamp) {
                            index++;
                        }
                        return index;
                    }
                    return 0;
                }
            });
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public int largestInclusiveTimestampIndex(final long timestamp) {
        try {
            return filerProvider.execute(-1, new FilerTransaction<Filer, Integer>() {
                @Override
                public Integer commit(Object lock, Filer filer) throws IOException {
                    if (filer != null) {
                        int index = readClosestId(lock, filer, timestamp);
                        if (index == -1) {
                            return -1;
                        }
                        if (index == timestampsLength) {
                            return timestampsLength - 1;
                        }
                        int lastId = lastId();
                        if (index > lastId) {
                            return lastId;
                        }
                        while (index <= lastId && readTimestamp(lock, filer, index) <= timestamp) {
                            index++;
                        }
                        return index - 1;
                    }
                    return -1;
                }
            });
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void stream(final Stream stream) throws Exception {
        filerProvider.execute(-1, new FilerTransaction<Filer, Void>() {
            @Override
            public Void commit(Object lock, Filer filer) throws IOException {
                int header = HEADER_SIZE_IN_BYTES + segmentWidth(LEVELS, SEGMENTS);
                int count = lastId() + 1;
                for (int i = 0; i < count; i++) {
                    long ts;
                    synchronized (lock) {
                        filer.seek(header + (8 * i));
                        ts = FilerIO.readLong(filer, "ts");
                    }
                    int index = timestampToIndex.execute(ts, false, streamTransaction);
                    try {
                        if (!stream.stream(new Entry(ts, index))) {
                            break;
                        }
                    } catch (Exception e) {
                        throw new IOException("Failed to stream", e);
                    }
                }
                return null;
            }
        });
    }

    @Override
    public void close() {
    }

    private static final KeyValueTransaction<Integer, Integer> streamTransaction = new KeyValueTransaction<Integer, Integer>() {
        @Override
        public Integer commit(KeyValueContext<Integer> context) throws IOException {
            Integer result = context.get();
            return result != null ? result : -1;
        }
    };

    public static interface TimeOrderAnomalyStream {

        void underflowOfSmallestTimestamp(long delta);

        void underflowOfLargestTimestamp(long delta);

    }
}
