package com.jivesoftware.os.miru.service.index.lab;

import com.google.common.collect.Maps;
import com.jivesoftware.os.lab.LABEnvironment;
import com.jivesoftware.os.lab.api.MemoryRawEntryFormat;
import com.jivesoftware.os.lab.api.NoOpFormatTransformerProvider;
import com.jivesoftware.os.lab.api.ValueIndex;
import com.jivesoftware.os.lab.api.ValueIndexConfig;
import com.jivesoftware.os.lab.api.rawhide.LABRawhide;
import com.jivesoftware.os.lab.io.BolBuffer;
import com.jivesoftware.os.lab.io.api.UIO;
import com.jivesoftware.os.miru.service.index.TimeIdIndex;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Semaphore;

/**
 *
 */
public class LabTimeIdIndex implements TimeIdIndex {

    private static final int NUM_SEMAPHORES = 1024;

    private final LABEnvironment environment;
    private final ValueIndex[] indexes;
    private final int maxEntriesPerIndex;
    private final long maxHeapPressureInBytes;
    private final boolean fsyncOnAppend;

    private final ConcurrentMap<Long, Cursor> cursors = Maps.newConcurrentMap();
    private final Semaphore semaphore = new Semaphore(NUM_SEMAPHORES, true);

    public LabTimeIdIndex(LABEnvironment environment,
        int keepNIndexes,
        int maxEntriesPerIndex,
        long maxHeapPressureInBytes,
        boolean fsyncOnAppend) throws Exception {
        this.environment = environment;
        this.indexes = new ValueIndex[keepNIndexes];
        this.maxEntriesPerIndex = maxEntriesPerIndex;
        this.maxHeapPressureInBytes = maxHeapPressureInBytes;
        this.fsyncOnAppend = fsyncOnAppend;

        List<String> names = environment.list();
        // sort descending
        Collections.sort(names, (o1, o2) -> Integer.compare(Integer.valueOf(o2), Integer.valueOf(o1)));
        for (int i = 0; i < names.size(); i++) {
            String name = names.get(i);
            if (i < keepNIndexes) {
                indexes[i] = open(name);
            } else {
                environment.remove(name);
            }
        }
        if (indexes[0] == null) {
            indexes[0] = open("1");
        }
    }

    private ValueIndex open(String name) throws Exception {
        return environment.open(new ValueIndexConfig(name, 4096, maxHeapPressureInBytes, 10 * 1024 * 1024, -1L, -1L,
            NoOpFormatTransformerProvider.NAME, LABRawhide.NAME, MemoryRawEntryFormat.NAME, 20));
    }

    @Override
    public void lookup(long version, long[] timestamps, int[] ids, long[] monotonics) throws Exception {
        semaphore.acquire();
        try {
            for (ValueIndex index : indexes) {
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
    public void allocate(long version, long[] timestamps, int[] ids, long[] monotonics, int lastIdHint, long largestTimestampHint) throws Exception {
        long count;
        semaphore.acquire();
        try {
            Cursor cursor = cursors.computeIfAbsent(version, k -> {
                try {
                    Cursor v = new Cursor();
                    for (ValueIndex index : indexes) {
                        if (index != null) {
                            index.get(
                                keyStream -> keyStream.key(0, UIO.longBytes(version), 0, 8),
                                (index1, key, timestamp1, tombstoned, version1, payload) -> {
                                    if (timestamp1 >= 0 && !tombstoned) {
                                        v.lastId = (int) timestamp1;
                                        v.largestTimestamp = version1;
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
                        v.largestTimestamp = largestTimestampHint;
                    }
                    return v;
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            });

            synchronized (cursor) {
                indexes[0].journaledAppend(valueStream -> {
                    for (int i = 0; i < timestamps.length; i++) {
                        byte[] key = UIO.longsBytes(new long[] { version, timestamps[i] });
                        cursor.lastId++;
                        cursor.largestTimestamp = Math.max(cursor.largestTimestamp, timestamps[i]);
                        ids[i] = cursor.lastId;
                        monotonics[i] = cursor.largestTimestamp;
                        boolean result = valueStream.stream(i, key, ids[i], false, monotonics[i], null);
                        result &= valueStream.stream(i, UIO.longBytes(version), ids[i], false, monotonics[i], null);
                        if (!result) {
                            return false;
                        }
                    }
                    return true;
                }, fsyncOnAppend, new BolBuffer(), new BolBuffer());
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
                ValueIndex[] rotated = new ValueIndex[indexes.length];
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
            for (ValueIndex index : indexes) {
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
        private long largestTimestamp = -1;
    }
}
