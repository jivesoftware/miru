package com.jivesoftware.os.miru.service.index.lab;

import com.google.common.collect.Maps;
import com.jivesoftware.os.lab.LABEnvironment;
import com.jivesoftware.os.lab.api.MemoryRawEntryFormat;
import com.jivesoftware.os.lab.api.NoOpFormatTransformerProvider;
import com.jivesoftware.os.lab.api.ValueIndex;
import com.jivesoftware.os.lab.api.ValueIndexConfig;
import com.jivesoftware.os.lab.api.rawhide.LABRawhide;
import com.jivesoftware.os.lab.guts.LABHashIndexType;
import com.jivesoftware.os.lab.io.BolBuffer;
import com.jivesoftware.os.lab.io.api.UIO;
import com.jivesoftware.os.miru.api.MiruPartitionCoord;
import com.jivesoftware.os.miru.service.index.TimeIdIndex;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Semaphore;
import org.apache.commons.lang.mutable.MutableInt;

/**
 *
 */
public class LabTimeIdIndex implements TimeIdIndex {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    private static final int NUM_SEMAPHORES = 1024;

    private final LABEnvironment environment;
    private final ValueIndex<byte[]>[] indexes;
    private final int maxEntriesPerIndex;
    private final long maxHeapPressureInBytes;
    private final LABHashIndexType hashIndexType;
    private final double hashIndexLoadFactor;
    private final boolean hashIndexEnabled;
    private final boolean fsyncOnAppend;
    private final boolean verboseLogging;

    private final ConcurrentMap<Long, Cursor> cursors = Maps.newConcurrentMap();
    private final Semaphore semaphore = new Semaphore(NUM_SEMAPHORES, true);

    public LabTimeIdIndex(LABEnvironment environment,
        int keepNIndexes,
        int maxEntriesPerIndex,
        long maxHeapPressureInBytes,
        LABHashIndexType hashIndexType,
        double hashIndexLoadFactor,
        boolean hashIndexEnabled,
        boolean fsyncOnAppend,
        boolean verboseLogging) throws Exception {

        this.environment = environment;
        this.indexes = new ValueIndex[keepNIndexes];
        this.maxEntriesPerIndex = maxEntriesPerIndex;
        this.maxHeapPressureInBytes = maxHeapPressureInBytes;
        this.hashIndexType = hashIndexType;
        this.hashIndexLoadFactor = hashIndexLoadFactor;
        this.hashIndexEnabled = hashIndexEnabled;
        this.fsyncOnAppend = fsyncOnAppend;
        this.verboseLogging = verboseLogging;

        List<String> names = environment.list();
        // sort descending
        Collections.sort(names, (o1, o2) -> Integer.compare(Integer.valueOf(o2), Integer.valueOf(o1)));
        for (int i = 0; i < names.size(); i++) {
            String name = names.get(i);
            if (i < keepNIndexes) {
                indexes[i] = open(name);
            } else {
                environment.remove(name, true);
            }
        }
        if (indexes[0] == null) {
            indexes[0] = open("1");
        }
    }

    public void close() throws Exception {
        for (int i = 0; i < indexes.length; i++) {
            if (indexes[i] != null) {
                indexes[i].close(false, false); //TODO ??
            }
        }
        environment.close();
    }

    private ValueIndex<byte[]> open(String name) throws Exception {
        return environment.open(new ValueIndexConfig(name, 4096, maxHeapPressureInBytes, 10 * 1024 * 1024, -1L, -1L,
            NoOpFormatTransformerProvider.NAME, LABRawhide.NAME, MemoryRawEntryFormat.NAME, 20, hashIndexType, hashIndexLoadFactor, hashIndexEnabled));
    }

    @Override
    public void lookup(long version, long[] timestamps, int[] ids, long[] monotonics) throws Exception {
        semaphore.acquire();
        try {
            for (ValueIndex<byte[]> index : indexes) {
                if (index != null) {
                    index.get(
                        keyStream -> {
                            for (int i = 0; i < timestamps.length; i++) {
                                byte[] key = UIO.longsBytes(new long[] { version, timestamps[i] });
                                if (!keyStream.key(i, key, 0, key.length)) {
                                    return false;
                                }
                            }
                            return true;
                        },
                        (index1, key, timestamp, tombstoned, version1, payload) -> {
                            if (timestamp >= 0 && !tombstoned && ids[index1] == -1) {
                                ids[index1] = (int) timestamp;
                                monotonics[index1] = version1;
                            }
                            return true;
                        },
                        false);
                }
            }
        } finally {
            semaphore.release();
        }
    }

    @Override
    public void allocate(MiruPartitionCoord coord,
        long version,
        long[] timestamps,
        int[] ids,
        long[] monotonics,
        int lastIdHint,
        long largestTimestampHint) throws Exception {

        long count;
        semaphore.acquire();
        try {
            Cursor cursor = cursors.computeIfAbsent(version, k -> {
                try {
                    Cursor v = new Cursor();
                    MutableInt depth = new MutableInt(0);
                    for (ValueIndex<byte[]> index : indexes) {
                        if (index != null) {
                            depth.increment();
                            index.get(
                                keyStream -> keyStream.key(0, UIO.longBytes(version), 0, 8),
                                (index1, key, timestamp1, tombstoned, version1, payload) -> {
                                    if (timestamp1 == -1) {
                                        // not in the index
                                    } else if (timestamp1 < -1) {
                                        LOG.error("Bad timeId cursor for coord:{} version:{} id:{} depth:{}", coord, version, timestamp1, depth);
                                    } else if (tombstoned) {
                                        LOG.error("Tombstoned timeId cursor for coord:{} version:{} id:{} depth:{}", coord, version, timestamp1, depth);
                                    } else {
                                        v.lastId = (int) timestamp1;
                                        v.lastTimestamp = version1;
                                    }
                                    return true;
                                },
                                false);
                            if (v.lastId != -1) {
                                break;
                            }
                        }
                    }
                    if (v.lastId == -1) {
                        v.lastId = lastIdHint;
                        v.lastTimestamp = largestTimestampHint;
                    } else if (v.lastId < lastIdHint) {
                        LOG.error("Lagging timeId cursor for coord:{} version:{} id:{} hint:{} depth:{}", coord, version, v.lastId, lastIdHint, depth);
                    }

                    if (verboseLogging) {
                        LOG.info("Loaded timeId cursor for coord:{} version:{} id:{} idHint:{} ts:{} tsHint:{} depth:{}",
                            coord, version, v.lastId, lastIdHint, v.lastTimestamp, largestTimestampHint, depth);
                    }
                    return v;
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            });

            synchronized (cursor) {
                indexes[0].append(valueStream -> {
                    for (int i = 0; i < timestamps.length; i++) {
                        byte[] key = UIO.longsBytes(new long[] { version, timestamps[i] });
                        cursor.lastId++;
                        cursor.lastTimestamp = Math.max(cursor.lastTimestamp + 1, timestamps[i]);
                        ids[i] = cursor.lastId;
                        monotonics[i] = cursor.lastTimestamp;
                        boolean result = valueStream.stream(i, key, ids[i], false, monotonics[i], null);
                        result &= valueStream.stream(i, UIO.longBytes(version), ids[i], false, monotonics[i], null);
                        if (!result) {
                            LOG.info("Aborted timeId append for coord:{} version:{} index:{} of:{}", coord, version, i, timestamps.length);
                            return false;
                        }
                    }
                    return true;
                }, fsyncOnAppend, new BolBuffer(), new BolBuffer());

                if (verboseLogging) {
                    indexes[0].get(keyStream -> keyStream.key(0, UIO.longBytes(version), 0, 8),
                        (index1, key, timestamp1, tombstoned, version1, payload) -> {
                            if (tombstoned) {
                                LOG.error("Unexpected tombstone for timeId cursor for coord:{} version:{} id:{} depth:{}", coord, version, timestamp1);
                            } else {
                                int lastId = (int) timestamp1;
                                if (lastId < cursor.lastId) {
                                    LOG.error("Lost timeId cursor for coord:{} version:{} id:{} depth:{}", coord, version, timestamp1);
                                }
                            }
                            return true;
                        },
                        false);
                    LOG.info("Advanced timeId cursor for coord:{} version:{} id:{} ts:{}", coord, version, cursor.lastId, cursor.lastTimestamp);
                }
            }

            count = indexes[0].count();
        } finally {
            semaphore.release();
        }

        if (count > maxEntriesPerIndex) {
            rotateIndexes();
        }
    }

    private void rotateIndexes() throws Exception {
        semaphore.acquire(NUM_SEMAPHORES);
        try {
            long count = indexes[0].count();
            if (count > maxEntriesPerIndex) {
                ValueIndex<byte[]>[] rotated = new ValueIndex[indexes.length];
                System.arraycopy(indexes, 0, rotated, 1, indexes.length - 1);
                int last = Integer.valueOf(indexes[0].name());
                rotated[0] = open(String.valueOf(last + 1));
                System.arraycopy(rotated, 0, indexes, 0, indexes.length);
            }
        } finally {
            semaphore.release(NUM_SEMAPHORES);
        }
    }

    public long count() throws Exception {
        semaphore.acquire();
        try {
            long count = 0;
            for (ValueIndex<byte[]> index : indexes) {
                if (index != null) {
                    count += index.count();
                }
            }
            return count;
        } finally {
            semaphore.release();
        }
    }

    private static class Cursor {
        private int lastId = -1;
        private long lastTimestamp = -1;
    }
}
