package com.jivesoftware.os.miru.service.index.delta;

import com.jivesoftware.os.miru.plugin.index.MiruTimeIndex;
import com.jivesoftware.os.miru.service.index.Mergeable;
import com.jivesoftware.os.miru.service.index.filer.MiruFilerTimeIndex;
import gnu.trove.list.array.TLongArrayList;
import gnu.trove.map.hash.TLongIntHashMap;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 *
 * @author jonathan.colt
 */
public class MiruDeltaTimeIndex implements MiruTimeIndex, Mergeable {

    private final MiruTimeIndex backingIndex;
    private final AtomicInteger baseId = new AtomicInteger(-1);
    private final AtomicInteger lastId = new AtomicInteger(-1);
    private final TLongArrayList timestamps = new TLongArrayList();
    private final TLongArrayList actualInsertionOrderTimestamps = new TLongArrayList();
    private final TLongIntHashMap timestampToId = new TLongIntHashMap(10, 0.5f, -1, -1);

    public MiruDeltaTimeIndex(MiruTimeIndex backingIndex) {
        this.backingIndex = backingIndex;
        clear();
    }

    private void clear() {
        actualInsertionOrderTimestamps.clear();
        timestamps.clear();
        timestampToId.clear();
        int id = this.backingIndex.lastId();
        baseId.set(id + 1);
        lastId.set(id);
    }

    @Override
    public boolean[] contains(List<Long> contains) throws Exception {
        if (timestamps.isEmpty()) {
            return backingIndex.contains(contains);
        } else {
            boolean[] result = new boolean[contains.size()];
            boolean delegate = false;
            for (int i = 0; i < contains.size(); i++) {
                if (timestampToId.contains(contains.get(i))) {
                    result[i] = true;
                } else {
                    delegate = true;
                }
            }
            if (delegate) {
                boolean[] merge = backingIndex.contains(contains);
                for (int i = 0; i < contains.size(); i++) {
                    result[i] |= merge[i];
                }
            }
            return result;
        }
    }

    @Override
    public int getClosestId(long timestamp) {
        if (timestamps.isEmpty()) {
            return backingIndex.getClosestId(timestamp);
        } else {
            int index = timestamps.binarySearch(timestamp);
            if (index >= 0) {
                return baseId.get() + index;
            } else if (index == -1) {
                return backingIndex.getClosestId(timestamp);
            } else {
                int insertIndex = -(index + 1);
                return baseId.get() + insertIndex;
            }
        }
    }

    @Override
    public int getExactId(long timestamp) throws Exception {
        if (timestamps.isEmpty()) {
            return backingIndex.getExactId(timestamp);
        } else {
            int got = timestampToId.get(timestamp);
            if (got == -1) {
                got = backingIndex.getExactId(timestamp);
            }
            return got;
        }
    }

    @Override
    public long getLargestTimestamp() {
        if (timestamps.isEmpty()) {
            return backingIndex.getLargestTimestamp();
        } else {
            return timestamps.get(timestamps.size() - 1);
        }
    }

    @Override
    public long getSmallestTimestamp() {
        if (timestamps.isEmpty()) {
            return backingIndex.getSmallestTimestamp();
        } else {
            long smallest = backingIndex.getSmallestTimestamp();
            if (smallest == Long.MAX_VALUE) {
                smallest = timestamps.get(0);
            }
            return smallest;
        }
    }

    @Override
    public long getTimestamp(int id) {
        if (timestamps.isEmpty()) {
            return backingIndex.getTimestamp(id);
        } else {
            if (id < baseId.get()) {
                return backingIndex.getTimestamp(id);
            } else {
                return timestamps.get(id - baseId.get());
            }
        }
    }

    @Override
    public int smallestExclusiveTimestampIndex(long timestamp) {
        if (timestamps.isEmpty()) {
            return backingIndex.smallestExclusiveTimestampIndex(timestamp);
        } else {
            int index = timestamps.binarySearch(timestamp);
            if (index >= 0) {
                int i = index;
                while (i < timestamps.size() && timestamps.get(i) <= timestamp) {
                    i++;
                }
                return baseId.get() + i;
            } else if (index == -1) {
                return backingIndex.smallestExclusiveTimestampIndex(timestamp);
            } else {
                int i = -(index + 1);
                while (i < timestamps.size() && timestamps.get(i) <= timestamp) {
                    i++;
                }
                return baseId.get() + i;
            }
        }
    }

    @Override
    public int largestInclusiveTimestampIndex(long timestamp) {
        if (timestamps.isEmpty()) {
            return backingIndex.largestInclusiveTimestampIndex(timestamp);
        } else {
            int index = timestamps.binarySearch(timestamp);
            if (index >= 0) {
                int i = index;
                while (i < timestamps.size() && timestamps.get(i) <= timestamp) {
                    i++;
                }
                return baseId.get() + i - 1;
            } else if (index == -1) {
                return backingIndex.largestInclusiveTimestampIndex(timestamp);
            } else {
                int i = -(index + 1);
                while (i < timestamps.size() && timestamps.get(i) <= timestamp) {
                    i++;
                }
                return baseId.get() + i - 1;
            }
        }
    }

    @Override
    public int lastId() {
        return lastId.get();
    }

    @Override
    public int[] nextId(long... ts) throws Exception {
        int[] result = new int[ts.length];
        long largestTimestamp = getLargestTimestamp();
        for (int i = 0; i < ts.length; i++) {
            if (ts[i] == -1) {
                result[i] = -1;
            } else {
                int id = lastId.incrementAndGet();
                result[i] = id;
                long addTimestamp = ts[i];
                if (addTimestamp < largestTimestamp) {
                    addTimestamp = largestTimestamp;
                } else {
                    largestTimestamp = ts[i];
                }
                timestampToId.put(ts[i], id);
                timestamps.add(addTimestamp);
                actualInsertionOrderTimestamps.add(ts[i]);
            }
        }
        return result;
    }

    @Override
    public void close() {
        backingIndex.close();
    }

    @Override
    public void merge() throws Exception {
        backingIndex.nextId(actualInsertionOrderTimestamps.toArray());
        clear();
    }
}
