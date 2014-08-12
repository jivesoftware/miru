package com.jivesoftware.os.miru.service.index.disk;

import com.google.common.collect.Lists;
import com.jivesoftware.os.miru.service.index.BulkExport;
import com.jivesoftware.os.miru.service.index.BulkImport;
import com.jivesoftware.os.miru.service.index.MiruFilerProvider;
import com.jivesoftware.os.miru.service.index.MiruTimeIndex;
import com.jivesoftware.os.jive.utils.io.Filer;
import com.jivesoftware.os.jive.utils.io.FilerIO;
import com.jivesoftware.os.jive.utils.io.RandomAccessFiler;
import com.jivesoftware.os.jive.utils.logger.MetricLogger;
import com.jivesoftware.os.jive.utils.logger.MetricLoggerFactory;
import com.jivesoftware.os.jive.utils.map.store.FileBackMapStore;
import com.jivesoftware.os.jive.utils.map.store.api.KeyValueStoreException;
import java.io.File;
import java.io.IOException;
import java.util.List;

/** @author jonathan */
public class MiruOnDiskTimeIndex implements MiruTimeIndex, BulkImport<MiruTimeIndex> {

    private static final MetricLogger log = MetricLoggerFactory.getLogger();

    private static final int headerSizeInBytes = 4 + 8 + 8 + 4 + 2 + 2;

    private static final int timestampSize = 8;
    private static final int searchIndexKeySize = 8;
    private static final int searchIndexValueSize = 4;

    private Filer filer;
    private int lastId;
    private long smallestTimestamp;
    private long largestTimestamp;
    private int timestampsLength;
    private int searchIndexLevels;
    private int searchIndexSegments;
    private int searchIndexSizeInBytes;

    private final MiruFilerProvider filerProvider;
    private final FileBackMapStore<Long, Integer> timestampToIndex;

    public MiruOnDiskTimeIndex(MiruFilerProvider filerProvider, File mapDirectory) throws IOException {
        this.filerProvider = filerProvider;
        this.timestampToIndex = new FileBackMapStore<Long, Integer>(mapDirectory.getAbsolutePath(), 8, 4, 32, 64, null) {

            private final int numPartitions = 4;

            @Override
            public String keyPartition(Long key) {
                return "partition-" + (key % numPartitions);
            }

            @Override
            public Iterable<String> keyPartitions() {
                List<String> partitions = Lists.newArrayListWithCapacity(numPartitions);
                for (int i = 0; i < numPartitions; i++) {
                    partitions.add(String.valueOf(i));
                }
                return partitions;
            }

            @Override
            public byte[] keyBytes(Long key) {
                return FilerIO.longBytes(key);
            }

            @Override
            public byte[] valueBytes(Integer value) {
                return FilerIO.intBytes(value);
            }

            @Override
            public Long bytesKey(byte[] bytes, int offset) {
                return FilerIO.bytesLong(bytes, offset);
            }

            @Override
            public Integer bytesValue(Long key, byte[] valueBytes, int offset) {
                return FilerIO.bytesInt(valueBytes, offset);
            }
        };

        File file = filerProvider.getBackingFile();
        if (file.length() > 0) {
            filer = filerProvider.getFiler(file.length());
            init();
        }
    }

    private void init() throws IOException {
        synchronized (filer.lock()) {
            this.filer.seek(0);
            this.lastId = FilerIO.readInt(filer, "lastId");
            this.smallestTimestamp = FilerIO.readLong(filer, "smallestTimestamp");
            this.largestTimestamp = FilerIO.readLong(filer, "largestTimestamp");
            this.timestampsLength = FilerIO.readInt(filer, "timestampsLength");
            this.searchIndexLevels = FilerIO.readShort(filer, "searchIndexLevels");
            this.searchIndexSegments = FilerIO.readShort(filer, "searchIndexSegments");
            this.searchIndexSizeInBytes = segmentWidth(searchIndexLevels, searchIndexSegments);
        }
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
    private static void segmentForSearch(MiruTimeIndex index, com.jivesoftware.os.jive.utils.io.Filer filer, int remainingLevels, int segments, long fp,
        int smallestId, int largestId) throws IOException {

        seekTo(filer, fp);

        final int delta = largestId - smallestId;

        // write index for this section
        int[] segmentAtIds = new int[segments];
        for (int i = 0; i < segments; i++) {
            segmentAtIds[i] = smallestId + delta * i / segments;
            //System.out.println("FP: " + filer.getFilePointer() + " index: " + index.getTimestamp(segmentAtIds[i]));
            FilerIO.writeLong(filer, index.getTimestamp(segmentAtIds[i]), "ts");
        }

        long subSegmentsFp = fp + segments * searchIndexKeySize;

        remainingLevels--;
        if (remainingLevels == 0) {
            // no more levels, so write id values
            for (int i = 0; i < segments; i++) {
                seekTo(filer, subSegmentsFp + i * searchIndexValueSize);
                //System.out.println("FP: " + filer.getFilePointer() + " value: " + segmentAtIds[i]);
                FilerIO.writeInt(filer, segmentAtIds[i], "id");
            }
        } else {
            // write subsections for next level
            int subSegmentWidth = segmentWidth(remainingLevels, segments);
            for (int i = 0; i < segments; i++) {
                int segmentSmallestId = segmentAtIds[i];
                int segmentLargestId = (i < segments - 1) ? segmentAtIds[i + 1] : largestId;
                segmentForSearch(index, filer, remainingLevels, segments, subSegmentsFp + i * subSegmentWidth, segmentSmallestId, segmentLargestId);
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

    //TODO delta should always be 0 if the algorithm is correct, so eventually we will remove all calls to this
    private static void seekTo(com.jivesoftware.os.jive.utils.io.Filer filer, long fp) throws IOException {
        if (fp - filer.getFilePointer() != 0) {
            System.out.println("Delta: " + (fp - filer.getFilePointer()));
        }
        filer.seek(fp);
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
    public long getTimestamp(int id) {
        if (id >= timestampsLength) {
            return 0l;
        }
        synchronized (filer.lock()) {
            try {
                filer.seek(headerSizeInBytes + searchIndexSizeInBytes + id * timestampSize);
                return FilerIO.readLong(filer, "ts");
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
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
    public int getClosestId(long timestamp) {
        synchronized (filer.lock()) {
            try {
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
                for (; id <= lastId && getTimestamp(id) < timestamp; id++) {
                }

                return id;
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    @Override
    public int getExactId(long timestamp) {
        try {
            Integer id = timestampToIndex.get(timestamp);
            return id != null ? id : -1;
        } catch (KeyValueStoreException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public boolean contains(long timestamp) {
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
    public Iterable<Entry> getEntries() {
        throw new UnsupportedOperationException("Disk index does not support getting entries");
    }

    @Override
    public long sizeInMemory() {
        return 0;
    }

    @Override
    public long sizeOnDisk() throws Exception {
        return filer.length() + timestampToIndex.sizeInBytes();
    }

    @Override
    public void close() {
        try {
            if (filer != null) {
                filer.close();
            }
        } catch (IOException e) {
            log.error("Failed to remove time index", e);
        }
    }

    @Override
    public void bulkImport(BulkExport<MiruTimeIndex> importItems) throws Exception {
        MiruTimeIndex index = importItems.bulkExport();

        long length;
        try (RandomAccessFiler filer = new RandomAccessFiler(filerProvider.getBackingFile(), "rw")) {
            synchronized (filer.lock()) {
                short levels = 3; // TODO - Config or allow it to be passed in?
                short segments = 10; // TODO - Config or allow it to be passed in?

                int lastId = index.lastId();
                filer.seek(0);
                FilerIO.writeInt(filer, lastId, "lastId");
                FilerIO.writeLong(filer, index.getSmallestTimestamp(), "smallestTimestamp");
                FilerIO.writeLong(filer, index.getLargestTimestamp(), "largestTimestamp");
                FilerIO.writeInt(filer, lastId + 1, "timestampsLength");
                FilerIO.writeShort(filer, levels, "searchIndexLevels");
                FilerIO.writeShort(filer, segments, "searchIndexSegments");

                seekTo(filer, headerSizeInBytes);
                segmentForSearch(index, filer, levels, segments, headerSizeInBytes, 0, lastId + 1);

                int searchIndexSizeInBytes = segmentWidth(levels, segments);
                seekTo(filer, headerSizeInBytes + searchIndexSizeInBytes);

                for (int id = 0; id <= lastId; id++) {
                    FilerIO.writeLong(filer, index.getTimestamp(id), "ts");
                }

                length = filer.length();
            }
        }

        for (Entry timeIndex : index.getEntries()) {
            timestampToIndex.add(timeIndex.time, timeIndex.index);
        }

        this.filer = filerProvider.getFiler(length);
        init();
    }

}
