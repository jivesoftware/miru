package com.jivesoftware.os.miru.service.index.lab;

import com.google.common.base.Optional;
import com.google.common.primitives.Bytes;
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
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author jonathan
 */
public class LabTimeIndex implements MiruTimeIndex {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

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
        ValueIndex rawTimestampToIndex)
        throws Exception {

        this.idProvider = idProvider;
        this.metaIndex = metaIndex;
        this.metaKey = metaKey;
        this.monotonicTimestampIndex = monotonicTimestampIndex;
        this.timeOrderAnomalyStream = timeOrderAnomalyStream;
        this.rawTimestampToIndex = rawTimestampToIndex;

        init();
    }

    private void init() throws Exception {
        final AtomicBoolean initialized = new AtomicBoolean(false);
        StackBuffer stackBuffer = new StackBuffer();
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

        if (!initialized.get()) {
            this.id.set(-1);
            this.smallestTimestamp = Long.MAX_VALUE;
            this.largestTimestamp = Long.MIN_VALUE;
            this.timestampsLength = 0;

            setMeta(this.id.get(), this.smallestTimestamp, this.largestTimestamp, this.timestampsLength, stackBuffer);

            LOG.inc("init-write>total");
        }
    }

    private void setMeta(int lastId, long smallestTimestamp, long largestTimestamp, int timestampsLength, StackBuffer stackBuffer) throws Exception {
        ByteArrayFiler metaFiler = new ByteArrayFiler();
        FilerIO.writeInt(metaFiler, lastId, "lastId", stackBuffer);
        FilerIO.writeLong(metaFiler, smallestTimestamp, "smallestTimestamp", stackBuffer);
        FilerIO.writeLong(metaFiler, largestTimestamp, "largestTimestamp", stackBuffer);
        FilerIO.writeInt(metaFiler, timestampsLength, "timestampsLength", stackBuffer);

        metaIndex.append((ValueStream stream) -> stream.stream(metaKey, System.currentTimeMillis(), false, idProvider.nextId(), metaFiler.getBytes()), true);
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
    public int[] nextId(StackBuffer stackBuffer, long... timestamps) throws Exception {
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
            timestampsLength = lastId + 1;
            setMeta(lastId, smallestTimestamp, largestTimestamp, timestampsLength, stackBuffer);

            long currentTime = System.currentTimeMillis();
            long version = idProvider.nextId();
            monotonicTimestampIndex.append((ValueStream stream) -> {
                int id1 = firstId;
                TLongIterator iter = monotonicTimestamps.iterator();
                while (iter.hasNext()) {
                    stream.stream(Bytes.concat(UIO.longBytes(iter.next()), UIO.intBytes(id1)), currentTime, false, version, null);
                    id1++;
                }
                return true;
            }, true);

            rawTimestampToIndex.append((ValueStream stream) -> {
                for (int i = 0; i < nextIds.length; i++) {
                    if (timestamps[i] != -1) {
                        stream.stream(UIO.longBytes(timestamps[i]), currentTime, false, version, UIO.intBytes(nextIds[i]));
                        LOG.inc("nextId>sets");
                    }
                }
                return true;
            }, true);
        }
        return nextIds;

    }

    /*
     Returns true index of activityTimestamp or where it would have been.
     */
    @Override
    public int getClosestId(final long timestamp, StackBuffer stackBuffer) throws Exception, InterruptedException {
        if (timestamp <= smallestTimestamp) {
            return 0;
        } else if (timestamp == largestTimestamp) {
            return lastId();
        } else if (timestamp > largestTimestamp) {
            return lastId() + 1;
        }

        int[] id = { 0 };
        monotonicTimestampIndex.rangeScan(UIO.longBytes(timestamp),
            null,
            (byte[] key, long ptimestamp, boolean tombstoned, long version, byte[] payload) -> {
                if (key != null) {
                    id[0] = UIO.bytesInt(key, 8);
                }
                return false;
            });
        return id[0];
    }

    private final KeyValueTransaction<Integer, Integer> exactIdTransaction = KeyValueContext::get;

    @Override
    public int getExactId(long timestamp, StackBuffer stackBuffer) throws Exception {
        int[] id = { -1 };
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
    public int smallestExclusiveTimestampIndex(final long timestamp, StackBuffer stackBuffer) throws Exception {
        int lastId = lastId();
        if (lastId < 0) {
            return 0;
        } else if (timestamp < smallestTimestamp) {
            return 0;
        } else if (timestamp >= largestTimestamp) {
            return lastId + 1;
        }

        int[] id = { 0 };
        monotonicTimestampIndex.rangeScan(UIO.longBytes(timestamp),
            null,
            (byte[] key, long payloadTimestamp, boolean tombstoned, long version, byte[] payload) -> {
                if (key != null) {
                    if (UIO.bytesLong(key, 0) <= timestamp) {
                        id[0] = UIO.bytesInt(key, 8) + 1;
                        return true;
                    } else {
                        id[0] = UIO.bytesInt(key, 8);
                    }
                }
                return false;
            });
        if (id[0] > lastId) {
            return lastId + 1;
        }
        return id[0];
    }

    @Override
    public int largestInclusiveTimestampIndex(final long timestamp, StackBuffer stackBuffer) throws Exception {
        int lastId = lastId();
        if (lastId < 0) {
            return -1;
        } else if (timestamp < smallestTimestamp) {
            return -1;
        } else if (timestamp > largestTimestamp) {
            return lastId;
        }

        int[] id = { -1 };
        monotonicTimestampIndex.rangeScan(UIO.longBytes(timestamp),
            null,
            (byte[] key, long payloadTimestamp, boolean tombstoned, long version, byte[] payload) -> {
                if (key != null) {
                    if (UIO.bytesLong(key, 0) <= timestamp) {
                        id[0] = UIO.bytesInt(key, 8);
                        return true;
                    } else {
                        id[0] = UIO.bytesInt(key, 8) - 1;
                    }
                }
                return false;
            });
        if (id[0] > lastId) {
            return lastId;
        }
        return id[0];
    }

    @Override
    public void close() {
    }

}
