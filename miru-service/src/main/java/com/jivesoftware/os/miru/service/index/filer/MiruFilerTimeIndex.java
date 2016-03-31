package com.jivesoftware.os.miru.service.index.filer;

import com.google.common.base.Optional;
import com.jivesoftware.os.filer.io.Filer;
import com.jivesoftware.os.filer.io.FilerIO;
import com.jivesoftware.os.filer.io.api.KeyValueContext;
import com.jivesoftware.os.filer.io.api.KeyValueStore;
import com.jivesoftware.os.filer.io.api.KeyValueTransaction;
import com.jivesoftware.os.filer.io.api.StackBuffer;
import com.jivesoftware.os.miru.plugin.index.MiruTimeIndex;
import com.jivesoftware.os.miru.plugin.solution.MiruTimeRange;
import com.jivesoftware.os.miru.service.index.MiruFilerProvider;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import gnu.trove.iterator.TLongIterator;
import gnu.trove.list.TLongList;
import gnu.trove.list.array.TLongArrayList;
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

    private final MiruFilerProvider<Long, Void> filerProvider;
    private final Optional<TimeOrderAnomalyStream> timeOrderAnomalyStream;
    private final KeyValueStore<Long, Integer> timestampToIndex;

    public MiruFilerTimeIndex(Optional<TimeOrderAnomalyStream> timeOrderAnomalyStream,
        MiruFilerProvider<Long, Void> filerProvider,
        KeyValueStore<Long, Integer> timestampToIndex)
        throws IOException, InterruptedException {

        this.filerProvider = filerProvider;
        this.timeOrderAnomalyStream = timeOrderAnomalyStream;
        this.timestampToIndex = timestampToIndex;

        init();
    }

    private void init() throws IOException, InterruptedException {
        final AtomicBoolean initialized = new AtomicBoolean(false);

        StackBuffer stackBuffer = new StackBuffer();
        //TODO consider using a custom CreateFiler in the KeyValueStore to handle the uninitialized case
        filerProvider.read(null, (monkey, filer, _stackBuffer, lock) -> {
            if (filer != null) {
                synchronized (lock) {
                    filer.seek(0);
                    MiruFilerTimeIndex.this.id.set(FilerIO.readInt(filer, "lastId", _stackBuffer));
                    MiruFilerTimeIndex.this.smallestTimestamp = FilerIO.readLong(filer, "smallestTimestamp", _stackBuffer);
                    MiruFilerTimeIndex.this.largestTimestamp = FilerIO.readLong(filer, "largestTimestamp", _stackBuffer);
                    MiruFilerTimeIndex.this.timestampsLength = FilerIO.readInt(filer, "timestampsLength", _stackBuffer);
                    MiruFilerTimeIndex.this.searchIndexLevels = FilerIO.readShort(filer, "searchIndexLevels", _stackBuffer);
                    MiruFilerTimeIndex.this.searchIndexSegments = FilerIO.readShort(filer, "searchIndexSegments", _stackBuffer);
                    MiruFilerTimeIndex.this.searchIndexSizeInBytes = segmentWidth(searchIndexLevels, searchIndexSegments);
                    initialized.set(true);
                }

            }
            return null;
        }, stackBuffer);
        log.inc("init-read>total");
        log.inc("init-read>bytes", HEADER_SIZE_IN_BYTES);

        if (!initialized.get()) {
            this.id.set(-1);
            this.smallestTimestamp = Long.MAX_VALUE;
            this.largestTimestamp = Long.MIN_VALUE;
            this.timestampsLength = 0;
            this.searchIndexLevels = LEVELS;
            this.searchIndexSegments = SEGMENTS;
            this.searchIndexSizeInBytes = segmentWidth(LEVELS, SEGMENTS);

            filerProvider.writeNewReplace((long) HEADER_SIZE_IN_BYTES, (monkey, filer, _stackBuffer, lock) -> {

                filer.seek(0);
                FilerIO.writeInt(filer, MiruFilerTimeIndex.this.id.get(), "lastId", _stackBuffer);
                FilerIO.writeLong(filer, MiruFilerTimeIndex.this.smallestTimestamp, "smallestTimestamp", _stackBuffer);
                FilerIO.writeLong(filer, MiruFilerTimeIndex.this.largestTimestamp, "largestTimestamp", _stackBuffer);
                FilerIO.writeInt(filer, MiruFilerTimeIndex.this.timestampsLength, "timestampsLength", _stackBuffer);
                FilerIO.writeShort(filer, MiruFilerTimeIndex.this.searchIndexLevels, "searchIndexLevels", _stackBuffer);
                FilerIO.writeShort(filer, MiruFilerTimeIndex.this.searchIndexSegments, "searchIndexSegments", _stackBuffer);

                return null;
            }, stackBuffer);

            log.inc("init-write>total");
            log.inc("init-write>bytes", HEADER_SIZE_IN_BYTES);

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
    private static void segmentForSearch(Filer filer, long searchIndexSizeInBytes, int remainingLevels, int segments, long fp, int smallestId, int largestId,
        StackBuffer stackBuffer)
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

            timestampAtIds[i] = FilerIO.readLong(filer, "ts", stackBuffer);
        }

        // now write the timestamps for this section
        filer.seek(fp);
        for (int i = 0; i < segments; i++) {
            FilerIO.writeLong(filer, timestampAtIds[i], "ts", stackBuffer);
        }

        // finally write the table for the next level, or the ids if this is the final level
        long subSegmentsFp = fp + segments * searchIndexKeySize;
        filer.seek(subSegmentsFp);

        remainingLevels--;
        if (remainingLevels == 0) {
            // no more levels, so write id values
            for (int i = 0; i < segments; i++) {
                FilerIO.writeInt(filer, segmentAtIds[i], "id", stackBuffer);
            }
        } else {
            // write subsections for next level
            int subSegmentWidth = segmentWidth(remainingLevels, segments);
            for (int i = 0; i < segments; i++) {
                int segmentSmallestId = segmentAtIds[i];
                int segmentLargestId = (i < segments - 1) ? segmentAtIds[i + 1] : largestId;
                segmentForSearch(filer, searchIndexSizeInBytes, remainingLevels, segments, subSegmentsFp + i * subSegmentWidth,
                    segmentSmallestId, segmentLargestId, stackBuffer);
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

    private long readTimestamp(Object lock, Filer filer, int id, StackBuffer stackBuffer) throws IOException {
        synchronized (lock) {
            filer.seek(HEADER_SIZE_IN_BYTES + searchIndexSizeInBytes + id * timestampSize);
            return FilerIO.readLong(filer, "ts", stackBuffer);
        }
    }

    @Override
    public int[] nextId(StackBuffer stackBuffer, long... timestamps) throws IOException, InterruptedException {
        final int[] nextIds = new int[timestamps.length];

        final TLongList monotonicTimestamps = new TLongArrayList(timestamps.length);
        int firstId = id.get() + 1;
        int lastId = -1;
        for (int i = 0; i < timestamps.length; i++) {
            long timestamp = timestamps[i];
            if (timestamp == -1) {
                nextIds[i] = -1;
            } else {
                nextIds[i] = id.incrementAndGet();
                lastId = nextIds[i];

                if (smallestTimestamp == Long.MAX_VALUE) {
                    smallestTimestamp = timestamp;
                }
                if (largestTimestamp < timestamp) {
                    largestTimestamp = timestamp;
                }
                monotonicTimestamps.add(largestTimestamp);

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
        }

        if (!monotonicTimestamps.isEmpty()) {
            long longs = (lastId + 1) * timestampSize;
            final int _firstId = firstId;
            final int _lastId = lastId;

            //TODO we should be concerned about potential corruption with failures during readWriteAutoGrow
            filerProvider.readWriteAutoGrow(HEADER_SIZE_IN_BYTES + searchIndexSizeInBytes + longs, (monkey, filer, _stackBuffer, lock) -> {
                synchronized (lock) {
                    filer.seek(0);
                    FilerIO.writeInt(filer, _lastId, "int", _stackBuffer);
                    FilerIO.writeLong(filer, smallestTimestamp, "smallestTimestamp", _stackBuffer);
                    FilerIO.writeLong(filer, largestTimestamp, "largestTimestamp", _stackBuffer);
                    timestampsLength = _lastId + 1;
                    FilerIO.writeInt(filer, timestampsLength, "timestampsLength", _stackBuffer);

                    filer.seek(HEADER_SIZE_IN_BYTES + searchIndexSizeInBytes + (_firstId * timestampSize));
                    TLongIterator iter = monotonicTimestamps.iterator();
                    while (iter.hasNext()) {
                        FilerIO.writeLong(filer, iter.next(), "long", _stackBuffer);
                    }

                    segmentForSearch(filer, searchIndexSizeInBytes, searchIndexLevels, searchIndexSegments, HEADER_SIZE_IN_BYTES, 0, _lastId + 1,
                        _stackBuffer);
                }
                return null;
            }, stackBuffer);
            log.inc("nextId>total");
            log.inc("nextId>bytes", HEADER_SIZE_IN_BYTES + searchIndexSizeInBytes + longs);

            Long[] keys = new Long[nextIds.length];
            for (int i = 0; i < nextIds.length; i++) {
                if (timestamps[i] != -1) {
                    keys[i] = timestamps[i];
                    log.inc("nextId>sets");
                }
            }
            timestampToIndex.multiExecute(keys, (keyValueContext, index) -> {
                keyValueContext.set(nextIds[index]);
            }, stackBuffer);
        }

        return nextIds;

    }

    /*
     Returns true index of activityTimestamp or where it would have been.
     */
    @Override
    public int getClosestId(final long timestamp, StackBuffer stackBuffer) throws IOException, InterruptedException {
        if (timestamp <= smallestTimestamp) {
            return 0;
        } else if (timestamp == largestTimestamp) {
            return lastId();
        } else if (timestamp > largestTimestamp) {
            return lastId() + 1;
        }

        int i = filerProvider.read(null, (monkey, filer, _stackBuffer, lock) -> {
            if (filer != null) {
                return readClosestId(lock, filer, timestamp, _stackBuffer);
            }
            return -1;
        }, stackBuffer);
        if (i < 0) {
            i = -(i + 1);
        }
        return i;
    }

    private int readClosestId(Object lock, Filer filer, long timestamp, StackBuffer stackBuffer) throws IOException {
        long fp = HEADER_SIZE_IN_BYTES;
        if (id.get() < 0) {
            return -1;
        } else if (timestamp > largestTimestamp) {
            return -(id.get() + 2);
        } else if (timestamp < smallestTimestamp) {
            return -1;
        }
        synchronized (lock) {
            filer.seek(fp);
            for (int remainingLevels = searchIndexLevels - 1; remainingLevels >= 0; remainingLevels--) {
                int segmentIndex = 0;
                for (; segmentIndex < searchIndexSegments; segmentIndex++) {
                    long segmentTimestamp = FilerIO.readLong(filer, "ts", stackBuffer);
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

            int id = FilerIO.readInt(filer, "id", stackBuffer);
            long lastId = lastId();
            long readTimestamp = readTimestamp(lock, filer, id, stackBuffer);
            while (id <= lastId && readTimestamp < timestamp) {
                id++;
                readTimestamp = readTimestamp(lock, filer, id, stackBuffer);
            }
            if (timestamp != readTimestamp) {
                return -(id + 1);
            }
            return id;
        }
    }

    private final KeyValueTransaction<Integer, Integer> exactIdTransaction = KeyValueContext::get;

    @Override
    public int getExactId(long timestamp, StackBuffer stackBuffer) throws Exception {
        Integer id = timestampToIndex.execute(timestamp, false, exactIdTransaction, stackBuffer);
        return id != null ? id : -1;
    }

    @Override
    public boolean[] contains(List<Long> timestamp, StackBuffer stackBuffer) throws Exception {
        return timestampToIndex.contains(timestamp, stackBuffer);
    }

    @Override
    public boolean intersects(MiruTimeRange timeRange) {
        return timeRange.smallestTimestamp <= getLargestTimestamp()
            && timeRange.largestTimestamp >= getSmallestTimestamp();
    }

    @Override
    public int smallestExclusiveTimestampIndex(final long timestamp, StackBuffer stackBuffer) throws IOException, InterruptedException {
        try {
            if (id.get() < 0) {
                return 0;
            }
            return filerProvider.read(null, (monkey, filer, _stackBuffer, lock) -> {
                if (filer != null) {
                    int index = readClosestId(lock, filer, timestamp, _stackBuffer);
                    if (index == -1) {
                        return 0;
                    }
                    if (index < -1) {
                        index = -(index + 1);
                    }
                    int lastId = lastId();
                    if (index > lastId) {
                        return lastId + 1;
                    }
                    while (index <= lastId && readTimestamp(lock, filer, index, _stackBuffer) <= timestamp) {
                        index++;
                    }
                    return index;
                }
                return 0;
            }, stackBuffer);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public int largestInclusiveTimestampIndex(final long timestamp, StackBuffer stackBuffer) throws IOException, InterruptedException {
        try {
            if (id.get() < 0) {
                return -1;
            }
            return filerProvider.read(null, (monkey, filer, _stackBuffer, lock) -> {
                if (filer != null) {
                    int index = readClosestId(lock, filer, timestamp, _stackBuffer);
                    if (index < 0) {
                        return -(index + 1) - 1;
                    }

                    if (index == timestampsLength) {
                        return timestampsLength - 1;
                    }
                    int lastId = lastId();
                    if (index > lastId) {
                        return lastId;
                    }

                    while (index <= lastId && readTimestamp(lock, filer, index, _stackBuffer) <= timestamp) {
                        index++;
                    }
                    return index - 1;
                }
                return -1;
            }, stackBuffer);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void close() {
    }

}
