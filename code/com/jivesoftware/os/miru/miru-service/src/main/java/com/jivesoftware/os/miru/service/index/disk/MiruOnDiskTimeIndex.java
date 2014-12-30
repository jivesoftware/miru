package com.jivesoftware.os.miru.service.index.disk;

import com.jivesoftware.os.filer.io.Filer;
import com.jivesoftware.os.filer.io.FilerIO;
import com.jivesoftware.os.filer.io.FilerTransaction;
import com.jivesoftware.os.filer.map.store.api.KeyValueContext;
import com.jivesoftware.os.filer.map.store.api.KeyValueStore;
import com.jivesoftware.os.filer.map.store.api.KeyValueTransaction;
import com.jivesoftware.os.jive.utils.logger.MetricLogger;
import com.jivesoftware.os.jive.utils.logger.MetricLoggerFactory;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.plugin.index.MiruTimeIndex;
import com.jivesoftware.os.miru.service.index.BulkExport;
import com.jivesoftware.os.miru.service.index.BulkImport;
import com.jivesoftware.os.miru.service.index.BulkStream;
import com.jivesoftware.os.miru.service.index.MiruFilerProvider;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/** @author jonathan */
public class MiruOnDiskTimeIndex implements MiruTimeIndex,
    BulkImport<Void, BulkStream<MiruTimeIndex.Entry>> {

    private static final MetricLogger log = MetricLoggerFactory.getLogger();

    private static final int headerSizeInBytes = 4 + 8 + 8 + 4 + 2 + 2;

    private static final int timestampSize = 8;
    private static final int searchIndexKeySize = 8;
    private static final int searchIndexValueSize = 4;

    private int lastId;
    private long smallestTimestamp;
    private long largestTimestamp;
    private int timestampsLength;
    private int searchIndexLevels;
    private int searchIndexSegments;
    private int searchIndexSizeInBytes;

    private final MiruFilerProvider filerProvider;
    private final KeyValueStore<Long, Integer> timestampToIndex;

    public MiruOnDiskTimeIndex(MiruFilerProvider filerProvider,
        KeyValueStore<Long, Integer> timestampToIndex)
        throws IOException {

        this.filerProvider = filerProvider;
        this.timestampToIndex = timestampToIndex;

        init();
    }

    private void init() throws IOException {
        filerProvider.execute(-1, new FilerTransaction<Filer, Void>() {
            @Override
            public Void commit(Filer filer) throws IOException {
                if (filer != null) {
                    filer.seek(0);
                    MiruOnDiskTimeIndex.this.lastId = FilerIO.readInt(filer, "lastId");
                    MiruOnDiskTimeIndex.this.smallestTimestamp = FilerIO.readLong(filer, "smallestTimestamp");
                    MiruOnDiskTimeIndex.this.largestTimestamp = FilerIO.readLong(filer, "largestTimestamp");
                    MiruOnDiskTimeIndex.this.timestampsLength = FilerIO.readInt(filer, "timestampsLength");
                    MiruOnDiskTimeIndex.this.searchIndexLevels = FilerIO.readShort(filer, "searchIndexLevels");
                    MiruOnDiskTimeIndex.this.searchIndexSegments = FilerIO.readShort(filer, "searchIndexSegments");
                    MiruOnDiskTimeIndex.this.searchIndexSizeInBytes = segmentWidth(searchIndexLevels, searchIndexSegments);
                }
                return null;
            }
        });
    }

    /**
     * Example: Levels=4, Segments=4, Timestamps=[0, 2048]
     * Note: Bracketed keys '{}' are long timestamps. Asterisked values '*' are integer ids.
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
    private void segmentForSearch(Filer filer, int remainingLevels, int segments, long fp, int smallestId, int largestId) throws IOException {
        final int delta = largestId - smallestId;

        // first select the ids for this section
        int[] segmentAtIds = new int[segments];
        for (int i = 0; i < segments; i++) {
            segmentAtIds[i] = smallestId + delta * i / segments;
        }

        // next get the timestamp for each selected id
        long[] timestampAtIds = new long[segments];
        for (int i = 0; i < segments; i++) {
            filer.seek(headerSizeInBytes + searchIndexSizeInBytes + segmentAtIds[i] * timestampSize);
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
                segmentForSearch(filer, remainingLevels, segments, subSegmentsFp + i * subSegmentWidth, segmentSmallestId, segmentLargestId);
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
        return lastId;
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
                public Long commit(Filer filer) throws IOException {
                    filer.seek(headerSizeInBytes + searchIndexSizeInBytes + id * timestampSize);
                    return FilerIO.readLong(filer, "ts");
                }
            });
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public int nextId(long timestamp) {
        throw new UnsupportedOperationException("Mem mapped indexes are readOnly");
    }

    /*
     Returns true index of activityTimestamp or where it would have been.
     */
    @Override
    public int getClosestId(final long timestamp) {
        try {
            return filerProvider.execute(-1, new FilerTransaction<Filer, Integer>() {
                @Override
                public Integer commit(Filer filer) throws IOException {
                    long fp = headerSizeInBytes;
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
                    while (id <= lastId && getTimestamp(id) < timestamp) {
                        id++;
                    }

                    return id;
                }
            });
        } catch (IOException e) {
            throw new RuntimeException(e);
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
    public boolean contains(long timestamp) throws Exception {
        int id = getExactId(timestamp);
        return id >= 0 && id < timestampsLength;
    }

    @Override
    public int smallestExclusiveTimestampIndex(long timestamp) {
        int index = getClosestId(timestamp);
        if (index == 0) {
            return 0;
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
    public int largestInclusiveTimestampIndex(long timestamp) {
        int index = getClosestId(timestamp);
        if (index == timestampsLength) {
            return timestampsLength - 1;
        }
        int lastId = lastId();
        if (index > lastId) {
            return lastId;
        }
        while (index <= lastId && getTimestamp(index) <= timestamp) {
            index++;
        }
        return index - 1;
    }

    @Override
    public long sizeInMemory() {
        return 0;
    }

    @Override
    public long sizeOnDisk() throws Exception {
        return -1;
    }

    @Override
    public void close() {
    }

    @Override
    public void bulkImport(final MiruTenantId tenantId, final BulkExport<Void, BulkStream<Entry>> export) throws Exception {
        //TODO expose to config
        filerProvider.execute(512, new FilerTransaction<Filer, Void>() {
            @Override
            public Void commit(final Filer filer) throws IOException {
                short levels = 3; // TODO - Config or allow it to be passed in?
                short segments = 10; // TODO - Config or allow it to be passed in?

                int searchIndexSizeInBytes = segmentWidth(levels, segments);
                filer.seek(headerSizeInBytes + searchIndexSizeInBytes);

                final AtomicInteger lastId = new AtomicInteger(-1);
                final AtomicLong smallestTimestamp = new AtomicLong(Long.MAX_VALUE);
                final AtomicLong largestTimestamp = new AtomicLong(Long.MIN_VALUE);
                try {
                    export.bulkExport(tenantId, new BulkStream<Entry>() {
                        @Override
                        public boolean stream(final Entry entry) throws Exception {
                            FilerIO.writeLong(filer, entry.time, "ts");
                            lastId.set(entry.index);
                            smallestTimestamp.compareAndSet(Long.MAX_VALUE, entry.time);
                            largestTimestamp.set(entry.time);
                            //TODO holy transactions, batman!
                            timestampToIndex.execute(entry.time, true, new KeyValueTransaction<Integer, Void>() {
                                @Override
                                public Void commit(KeyValueContext<Integer> keyValueContext) throws IOException {
                                    keyValueContext.set(entry.index);
                                    return null;
                                }
                            });
                            return true;
                        }
                    });
                } catch (Exception e) {
                    throw new IOException("Failed to stream entries", e);
                }

                filer.seek(0);
                FilerIO.writeInt(filer, lastId.get(), "lastId");
                FilerIO.writeLong(filer, smallestTimestamp.get(), "smallestTimestamp");
                FilerIO.writeLong(filer, largestTimestamp.get(), "largestTimestamp");
                FilerIO.writeInt(filer, lastId.get() + 1, "timestampsLength");
                FilerIO.writeShort(filer, levels, "searchIndexLevels");
                FilerIO.writeShort(filer, segments, "searchIndexSegments");

                filer.seek(headerSizeInBytes);
                segmentForSearch(filer, levels, segments, headerSizeInBytes, 0, lastId.get() + 1);

                return null;
            }
        });

        init();
    }

}
