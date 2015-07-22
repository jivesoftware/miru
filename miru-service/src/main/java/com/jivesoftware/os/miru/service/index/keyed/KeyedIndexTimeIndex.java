package com.jivesoftware.os.miru.service.index.keyed;

import com.google.common.base.Optional;
import com.jivesoftware.os.filer.io.FilerIO;
import com.jivesoftware.os.miru.plugin.index.MiruTimeIndex;
import com.jivesoftware.os.miru.service.index.TimeOrderAnomalyStream;
import com.jivesoftware.os.miru.service.stream.KeyedIndex;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import gnu.trove.list.TLongList;
import gnu.trove.list.array.TLongArrayList;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 *
 */
public class KeyedIndexTimeIndex implements MiruTimeIndex {

    private static final MetricLogger log = MetricLoggerFactory.getLogger();

    private final AtomicInteger id = new AtomicInteger(-1);
    private long smallestTimestamp;
    private long largestTimestamp;

    private final KeyedIndex alignmentKeyedIndex;
    private final KeyedIndex[] timestampKeyedIndexes;
    private final Optional<TimeOrderAnomalyStream> timeOrderAnomalyStream;

    public KeyedIndexTimeIndex(KeyedIndex alignmentKeyedIndex,
        KeyedIndex[] timestampKeyedIndexes,
        Optional<TimeOrderAnomalyStream> timeOrderAnomalyStream)
        throws IOException {

        this.alignmentKeyedIndex = alignmentKeyedIndex;
        this.timestampKeyedIndexes = timestampKeyedIndexes;
        this.timeOrderAnomalyStream = timeOrderAnomalyStream;

        init();
    }

    private void init() throws IOException {
        final AtomicBoolean initialized = new AtomicBoolean(false);

        this.id.set(-1);
        this.smallestTimestamp = Long.MAX_VALUE;
        this.largestTimestamp = Long.MIN_VALUE;

        alignmentKeyedIndex.stream(null, null, (key, value) -> {
            if (key != null && value != null) {
                this.smallestTimestamp = FilerIO.bytesLong(value);
            }
            return false;
        });

        alignmentKeyedIndex.reverseStream(null, null, (key, value) -> {
            if (key != null && value != null) {
                this.id.set(FilerIO.bytesInt(key));
                this.largestTimestamp = FilerIO.bytesLong(value);
            }
            return false;
        });

        if (!initialized.get()) {
            this.id.set(-1);
        }
    }

    private KeyedIndex timestampKeyedIndex(long timestamp) {
        return timestampKeyedIndexes[Math.abs(Long.hashCode(timestamp) % timestampKeyedIndexes.length)];
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
    public long getTimestamp(int id) {
        byte[] timestampBytes = alignmentKeyedIndex.get(FilerIO.intBytes(id));
        log.inc("get>total");
        return timestampBytes == null ? 0L : FilerIO.bytesLong(timestampBytes);
    }

    @Override
    public int[] nextId(long... timestamps) throws IOException {
        final int[] nextIds = new int[timestamps.length];

        final TLongList monotonicTimestamps = new TLongArrayList(timestamps.length);
        for (int i = 0; i < timestamps.length; i++) {
            long timestamp = timestamps[i];
            if (timestamp == -1) {
                nextIds[i] = -1;
            } else {
                nextIds[i] = id.incrementAndGet();

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
            log.inc("nextId>total");

            for (int i = 0; i < nextIds.length; i++) {
                long timestamp = timestamps[i];
                if (timestamp != -1) {
                    log.inc("nextId>sets");
                    byte[] idBytes = FilerIO.intBytes(nextIds[i]);
                    alignmentKeyedIndex.put(idBytes, FilerIO.longBytes(monotonicTimestamps.get(i)));
                    timestampKeyedIndex(timestamp).put(FilerIO.longBytes(timestamp), idBytes);
                }
            }
        }

        return nextIds;
    }

    /*
     Returns true index of activityTimestamp or where it would have been.
     */
    @Override
    public int getClosestId(long timestamp) {
        byte[] timestampBytes = FilerIO.longBytes(timestamp);
        byte[][] idBytes = new byte[1][];
        boolean[] miss = new boolean[1];
        alignmentKeyedIndex.stream(timestampBytes, null, (keyBytes, valueBytes) -> {
            idBytes[0] = valueBytes;
            miss[0] = !Arrays.equals(timestampBytes, keyBytes);
            return false;
        });
        if (idBytes[0] == null) {
            return -1;
        }
        if (miss[0]) {
            return -FilerIO.bytesInt(idBytes[0]);
        } else {
            return FilerIO.bytesInt(idBytes[0]);
        }
    }

    @Override
    public int getExactId(long timestamp) throws Exception {
        byte[] idBytes = timestampKeyedIndex(timestamp).get(FilerIO.longBytes(timestamp));
        return idBytes == null ? -1 : FilerIO.bytesInt(idBytes);
    }

    @Override
    public boolean[] contains(List<Long> timestamps) throws Exception {
        boolean[] contained = new boolean[timestamps.size()];
        int i = 0;
        for (Long timestamp : timestamps) {
            contained[i] = timestampKeyedIndex(timestamp).contains(FilerIO.longBytes(timestamp));
        }
        return contained;
    }

    @Override
    public int smallestExclusiveTimestampIndex(final long timestamp) {
        if (id.get() < 0) {
            return 0;
        }
        int index = getClosestId(timestamp);
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
        while (index <= lastId && getTimestamp(index) <= timestamp) {
            index++;
        }
        return index;
    }

    @Override
    public int largestInclusiveTimestampIndex(final long timestamp) {
        int lastId = id.get();
        if (lastId < 0) {
            return -1;
        }
        int index = getClosestId(timestamp);
        if (index < 0) {
            return -(index + 1) - 1;
        }

        if (index >= lastId) {
            return lastId;
        }

        while (index <= lastId && getTimestamp(index) <= timestamp) {
            index++;
        }
        return index - 1;
    }

    @Override
    public void close() {
    }
}
