package com.jivesoftware.os.miru.service.index.lab;

import com.google.common.base.Optional;
import com.jivesoftware.os.filer.io.ByteArrayFiler;
import com.jivesoftware.os.filer.io.Filer;
import com.jivesoftware.os.filer.io.FilerIO;
import com.jivesoftware.os.filer.io.api.KeyValueContext;
import com.jivesoftware.os.filer.io.api.KeyValueTransaction;
import com.jivesoftware.os.filer.io.api.StackBuffer;
import com.jivesoftware.os.jive.utils.ordered.id.OrderIdProvider;
import com.jivesoftware.os.lab.api.ValueIndex;
import com.jivesoftware.os.lab.api.ValueStream;
import com.jivesoftware.os.lab.io.api.UIO;
import com.jivesoftware.os.miru.plugin.index.MiruTimeIndex;
import com.jivesoftware.os.miru.plugin.solution.MiruTimeRange;
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
public class LabTimeIndex implements MiruTimeIndex {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

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

    private final OrderIdProvider idProvider;
    private final ValueIndex metaIndex;
    private final byte[] metaKey;
    private final ValueIndex monotonicTimestampIndex;
    private final Optional<TimeOrderAnomalyStream> timeOrderAnomalyStream;
    private final ValueIndex rawTimestampToIndex;

    public LabTimeIndex(OrderIdProvider idProvider,
        Optional<TimeOrderAnomalyStream> timeOrderAnomalyStream,
        ValueIndex metaIndex,
        byte[] metaKey,
        ValueIndex monotonicTimestampIndex,
        ValueIndex rawTimestampToIndex,
        StackBuffer stackBuffer)
        throws Exception, InterruptedException {

        this.idProvider = idProvider;
        this.metaIndex = metaIndex;
        this.metaKey = metaKey;
        this.monotonicTimestampIndex = monotonicTimestampIndex;
        this.timeOrderAnomalyStream = timeOrderAnomalyStream;
        this.rawTimestampToIndex = rawTimestampToIndex;

        init(stackBuffer);
    }

    private void init(StackBuffer stackBuffer) throws Exception, InterruptedException {
        final AtomicBoolean initialized = new AtomicBoolean(false);

        //TODO consider using a custom CreateFiler in the KeyValueStore to handle the uninitialized case
        metaIndex.get(metaKey, (byte[] key, long timestamp, boolean tombstoned, long version, byte[] payload) -> {
            if (payload != null && !tombstoned) {
                Filer filer = new ByteArrayFiler(payload);
                LabTimeIndex.this.id.set(FilerIO.readInt(filer, "lastId", stackBuffer));
                LabTimeIndex.this.smallestTimestamp = FilerIO.readLong(filer, "smallestTimestamp", stackBuffer);
                LabTimeIndex.this.largestTimestamp = FilerIO.readLong(filer, "largestTimestamp", stackBuffer);
                LabTimeIndex.this.timestampsLength = FilerIO.readInt(filer, "timestampsLength", stackBuffer);
                initialized.set(true);

            }
            return true;
        });

        LOG.inc("init-read>total");
        LOG.inc("init-read>bytes", HEADER_SIZE_IN_BYTES);

        if (!initialized.get()) {
            this.id.set(-1);
            this.smallestTimestamp = Long.MAX_VALUE;
            this.largestTimestamp = Long.MIN_VALUE;
            this.timestampsLength = 0;

            setMeta(this.id.get(), this.smallestTimestamp, this.largestTimestamp, this.timestampsLength, stackBuffer);

            LOG.inc("init-write>total");
            LOG.inc("init-write>bytes", HEADER_SIZE_IN_BYTES);

        }
    }

    private void setMeta(int lastId, long smallestTimestamp, long largestTimestamp, int timestampsLength, StackBuffer stackBuffer) throws Exception {
        ByteArrayFiler metaFiler = new ByteArrayFiler();
        FilerIO.writeInt(metaFiler, lastId, "lastId", stackBuffer);
        FilerIO.writeLong(metaFiler, smallestTimestamp, "smallestTimestamp", stackBuffer);
        FilerIO.writeLong(metaFiler, largestTimestamp, "largestTimestamp", stackBuffer);
        FilerIO.writeInt(metaFiler, timestampsLength, "timestampsLength", stackBuffer);

        metaIndex.append((ValueStream stream) -> stream.stream(metaKey, System.currentTimeMillis(), false, idProvider.nextId(), metaFiler.getBytes()));
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

    @Override
    public int[] nextId(StackBuffer stackBuffer, long... timestamps) throws Exception, InterruptedException {
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

            timestampsLength = _lastId + 1;
            setMeta(_lastId, smallestTimestamp, largestTimestamp, timestampsLength, stackBuffer);

            long currentTime = System.currentTimeMillis();
            long version = idProvider.nextId();
            monotonicTimestampIndex.append((ValueStream stream) -> {
                int id1 = firstId;
                TLongIterator iter = monotonicTimestamps.iterator();
                while (iter.hasNext()) {
                    stream.stream(UIO.longBytes(iter.next()), currentTime, false, version, UIO.intBytes(id1));
                    id1++;
                }
                return true;
            });

            rawTimestampToIndex.append((ValueStream stream) -> {
                for (int i = 0; i < nextIds.length; i++) {
                    if (timestamps[i] != -1) {
                        stream.stream(UIO.longBytes(timestamps[i]), currentTime, false, version, UIO.intBytes(nextIds[i]));
                        LOG.inc("nextId>sets");
                    }
                }
                return true;
            });

        }
        return nextIds;

    }

    /*
     Returns true index of activityTimestamp or where it would have been.
     */
    @Override
    public int getClosestId(final long timestamp, StackBuffer stackBuffer) throws Exception, InterruptedException {
        int[] id = {-1};
        monotonicTimestampIndex.rangeScan(UIO.longBytes(timestamp),
            null,
            (byte[] key, long ptimestamp, boolean tombstoned, long version, byte[] payload) -> {
                if (payload != null) {
                    if (UIO.bytesLong(key) == timestamp) {
                        id[0] = UIO.bytesInt(payload);
                    } else {
                        id[0] = -(UIO.bytesInt(payload) + 1);
                    }
                }
                return false;
            });
        return id[0];
    }

    private final KeyValueTransaction<Integer, Integer> exactIdTransaction = KeyValueContext::get;

    @Override
    public int getExactId(long timestamp, StackBuffer stackBuffer) throws Exception {
        int[] id = {-1};
        rawTimestampToIndex.get(UIO.longBytes(timestamp), (byte[] key, long ptimestamp, boolean tombstoned, long version, byte[] payload) -> {
            if (payload != null) {
                id[0] = UIO.bytesInt(payload);
            }
            return false;
        });
        return id[0];
    }

    @Override
    public boolean[] contains(List<Long> timestamp, StackBuffer stackBuffer) throws Exception {

        boolean[] contained = new boolean[timestamp.size()];
        for (int i = 0; i < contained.length; i++) {
            Long ts = timestamp.get(i);
            if (ts != null && ts != -1) {
                int ci = i;
                rawTimestampToIndex.get(UIO.longBytes(ts),
                    (byte[] key, long ptimestamp, boolean tombstoned, long version, byte[] payload) -> {
                        if (payload != null) {
                            contained[ci] = true;
                        }
                        return false;
                    });
            }
        }
        return contained;
    }

    @Override
    public boolean intersects(MiruTimeRange timeRange) {
        return timeRange.smallestTimestamp <= getLargestTimestamp()
            && timeRange.largestTimestamp >= getSmallestTimestamp();
    }

    @Override
    public int smallestExclusiveTimestampIndex(final long timestamp, StackBuffer stackBuffer) throws Exception, InterruptedException {
        try {
            if (id.get() < 0) {
                return 0;
            }
            int[] id = {-1};
            monotonicTimestampIndex.rangeScan(UIO.longBytes(timestamp),
                null,
                (byte[] key, long ptimestamp, boolean tombstoned, long version, byte[] payload) -> {
                    if (payload != null) {
                        if (UIO.bytesLong(key) <= timestamp) {
                            id[0] = UIO.bytesInt(payload) + 1;
                            return true;
                        } else {
                            id[0] = UIO.bytesInt(payload);
                        }
                    }
                    return false;
                });
            if (id[0] == -1) {
                return 0;
            }
            int lastId = lastId();
            if (id[0] > lastId) {
                return lastId + 1;
            }
            return id[0];

            return monotonicTimestampIndex.read(null, (monkey, filer, _stackBuffer, lock) -> {
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
            return monotonicTimestampIndex.read(null, (monkey, filer, _stackBuffer, lock) -> {
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

    public static interface TimeOrderAnomalyStream {

        void underflowOfSmallestTimestamp(long delta);

        void underflowOfLargestTimestamp(long delta);

    }
}
