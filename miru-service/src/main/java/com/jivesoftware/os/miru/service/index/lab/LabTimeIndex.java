package com.jivesoftware.os.miru.service.index.lab;

import com.google.common.primitives.Bytes;
import com.jivesoftware.os.filer.io.ByteArrayFiler;
import com.jivesoftware.os.filer.io.ByteBufferBackedFiler;
import com.jivesoftware.os.filer.io.Filer;
import com.jivesoftware.os.filer.io.FilerIO;
import com.jivesoftware.os.filer.io.api.KeyValueContext;
import com.jivesoftware.os.filer.io.api.KeyValueTransaction;
import com.jivesoftware.os.filer.io.api.StackBuffer;
import com.jivesoftware.os.jive.utils.ordered.id.OrderIdProvider;
import com.jivesoftware.os.lab.api.ValueIndex;
import com.jivesoftware.os.lab.io.BolBuffer;
import com.jivesoftware.os.lab.io.api.UIO;
import com.jivesoftware.os.miru.plugin.index.MiruTimeIndex;
import com.jivesoftware.os.miru.plugin.solution.MiruTimeRange;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
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
    private final ValueIndex<byte[]> metaIndex;
    private final byte[] metaKey;
    private final ValueIndex<byte[]> monotonicTimestampIndex;
    private final ValueIndex<byte[]> rawTimestampToIndex;

    public LabTimeIndex(OrderIdProvider idProvider,
        ValueIndex<byte[]> metaIndex,
        byte[] metaKey,
        ValueIndex<byte[]> monotonicTimestampIndex,
        ValueIndex<byte[]> rawTimestampToIndex)
        throws Exception {

        this.idProvider = idProvider;
        this.metaIndex = metaIndex;
        this.metaKey = metaKey;
        this.monotonicTimestampIndex = monotonicTimestampIndex;
        this.rawTimestampToIndex = rawTimestampToIndex;

        init();
    }

    private void init() throws Exception {
        final AtomicBoolean initialized = new AtomicBoolean(false);
        StackBuffer stackBuffer = new StackBuffer();
        metaIndex.get(
            (keyStream) -> keyStream.key(0, metaKey, 0, metaKey.length),
            (index, key, timestamp, tombstoned, version, payload) -> {
                if (payload != null && !tombstoned) {
                    Filer filer = new ByteBufferBackedFiler(payload.asByteBuffer());
                    LabTimeIndex.this.id.set(FilerIO.readInt(filer, "lastId", stackBuffer));
                    LabTimeIndex.this.smallestTimestamp = FilerIO.readLong(filer, "smallestTimestamp", stackBuffer);
                    LabTimeIndex.this.largestTimestamp = FilerIO.readLong(filer, "largestTimestamp", stackBuffer);
                    LabTimeIndex.this.timestampsLength = FilerIO.readInt(filer, "timestampsLength", stackBuffer);
                    initialized.set(true);

                }
                return true;
            },
            true
        );

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

        long timestamp = System.currentTimeMillis();
        long version = idProvider.nextId();
        metaIndex.append(stream -> stream.stream(-1, metaKey, timestamp, false, version, metaFiler.getBytes()), true, new BolBuffer(), new BolBuffer());
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
    public void nextId(StackBuffer stackBuffer, long[] timestamps, int[] ids, long[] monotonics) throws Exception {
        int lastId = id.get();
        for (int i = 0; i < timestamps.length; i++) {
            if (timestamps[i] != -1) {
                if (smallestTimestamp == Long.MAX_VALUE) {
                    smallestTimestamp = monotonics[i];
                }
                if (largestTimestamp < monotonics[i]) {
                    largestTimestamp = monotonics[i];
                }
            }
            if (ids[i] > lastId) {
                lastId = ids[i];
            }
        }
        id.set(lastId);

        timestampsLength = lastId + 1;
        setMeta(lastId, smallestTimestamp, largestTimestamp, timestampsLength, stackBuffer);

        BolBuffer entryBuffer = new BolBuffer();
        BolBuffer keyBuffer = new BolBuffer();
        long currentTime = System.currentTimeMillis();
        long version = idProvider.nextId();
        monotonicTimestampIndex.append(stream -> {
            for (int i = 0; i < ids.length; i++) {
                if (ids[i] != -1 && monotonics[i] != -1) {
                    if (!stream.stream(-1, Bytes.concat(UIO.longBytes(monotonics[i]), FilerIO.intBytes(ids[i])), currentTime, false, version, null)) {
                        return false;
                    }
                }
            }
            return true;
        }, true, entryBuffer, keyBuffer);

        rawTimestampToIndex.append(stream -> {
            for (int i = 0; i < ids.length; i++) {
                if (timestamps[i] != -1 && ids[i] != -1) {
                    if (!stream.stream(-1, UIO.longBytes(timestamps[i]), currentTime, false, version, FilerIO.intBytes(ids[i]))) {
                        return false;
                    }
                    LOG.inc("nextId>sets");
                }
            }
            return true;
        }, true, entryBuffer, keyBuffer);
    }

    /*
     Returns true index of activityTimestamp or where it would have been.
     */
    @Override
    public int getClosestId(final long timestamp, StackBuffer stackBuffer) throws Exception {
        if (timestamp <= smallestTimestamp) {
            return 0;
        } else if (timestamp == largestTimestamp) {
            return lastId();
        } else if (timestamp > largestTimestamp) {
            return lastId() + 1;
        }

        int[] id = { 0 };
        monotonicTimestampIndex.rangeScan(UIO.longBytes(timestamp), null, (index, key, payloadTimestamp, tombstoned, version, payload) -> {
            if (key != null) {
                id[0] = key.getInt(8);
            }
            return false;
        }, true);
        return id[0];
    }

    private final KeyValueTransaction<Integer, Integer> exactIdTransaction = KeyValueContext::get;

    @Override
    public int getExactId(long timestamp, StackBuffer stackBuffer) throws Exception {
        int[] id = { -1 };
        rawTimestampToIndex.get(
            (keyStream) -> keyStream.key(0, UIO.longBytes(timestamp), 0, 8),
            (index, key, payloadTimestamp, tombstoned, version, payload) -> {
                if (payload != null) {
                    id[0] = payload.getInt(0);
                }
                return false;
            },
            true
        );
        return id[0];
    }

    @Override
    public boolean[] contains(List<Long> timestamp, StackBuffer stackBuffer) throws Exception {

        boolean[] contained = new boolean[timestamp.size()];
        rawTimestampToIndex.get(
            keyStream -> {
                for (int i = 0; i < contained.length; i++) {
                    Long ts = timestamp.get(i);
                    if (ts != null && ts != -1) {
                        byte[] key = UIO.longBytes(ts);
                        if (!keyStream.key(i, key, 0, key.length)) {
                            return false;
                        }
                    }
                }
                return true;
            },
            (index, key, timestamp1, tombstoned, version, payload) -> {
                if (payload != null) {
                    contained[index] = true;
                }
                return true;
            },
            true
        );
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
        monotonicTimestampIndex.rangeScan(
            UIO.longBytes(timestamp),
            null,
            (index, key, payloadTimestamp, tombstoned, version, payload) -> {
                if (key != null) {
                    if (key.getLong(0) <= timestamp) {
                        id[0] = key.getInt(8) + 1;
                        return true;
                    } else {
                        id[0] = key.getInt(8);
                    }
                }
                return false;
            },
            true
        );
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
        monotonicTimestampIndex.rangeScan(
            UIO.longBytes(timestamp),
            null,
            (index, key, payloadTimestamp, tombstoned, version, payload) -> {
                if (key != null) {
                    if (key.getLong(0) <= timestamp) {
                        id[0] = key.getInt(8);
                        return true;
                    } else {
                        id[0] = key.getInt(8) - 1;
                    }
                }
                return false;
            },
            true
        );
        if (id[0] > lastId) {
            return lastId;
        }
        return id[0];
    }

    @Override
    public void close() {
    }

    @Override
    public String toString() {
        return "LabTimeIndex{" +
            "id=" + id +
            ", smallestTimestamp=" + smallestTimestamp +
            ", largestTimestamp=" + largestTimestamp +
            ", timestampsLength=" + timestampsLength +
            '}';
    }
}
