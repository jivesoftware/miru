package com.jivesoftware.os.miru.service.index.disk;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.jivesoftware.os.miru.api.activity.MiruActivity;
import com.jivesoftware.os.miru.service.index.BulkExport;
import com.jivesoftware.os.miru.service.index.BulkImport;
import com.jivesoftware.os.miru.service.index.MiruActivityIndex;
import com.jivesoftware.os.miru.service.index.MiruFilerProvider;
import com.jivesoftware.os.jive.utils.io.Filer;
import com.jivesoftware.os.jive.utils.io.FilerIO;
import com.jivesoftware.os.jive.utils.io.RandomAccessFiler;
import com.jivesoftware.os.jive.utils.logger.MetricLogger;
import com.jivesoftware.os.jive.utils.logger.MetricLoggerFactory;
import java.io.File;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * int indexSize seek 0
 * long[] offsetIndex seek 4;  (i * 8) read long have offset to activity i.
 *
 * @author jonathan
 * @deprecated Activity index needs to support set() for repairs/removals.
 */
@Deprecated
public class MiruOnDiskActivityIndex implements MiruActivityIndex, BulkImport<MiruActivity[]> {

    private static final MetricLogger log = MetricLoggerFactory.getLogger();

    private final MiruFilerProvider filerProvider;
    private final ObjectMapper objectMapper;
    private final AtomicInteger indexSize;

    private Filer filer;

    public MiruOnDiskActivityIndex(MiruFilerProvider filerProvider, ObjectMapper objectMapper) throws IOException {
        this.filerProvider = filerProvider;
        this.objectMapper = objectMapper;
        this.indexSize = new AtomicInteger(0);

        File file = filerProvider.getBackingFile();
        if (file.length() > 0) {
            filer = filerProvider.getFiler(file.length());
        }
    }

    @Override
    public MiruActivity get(int index) {
        checkArgument(index >= 0 && index < capacity(), "Index parameter is out of bounds. The value " + index + " must be >=0 and <" + capacity());
        try {
            byte[] rawActivity;
            synchronized (filer.lock()) {
                filer.seek(4 + (index * 8));
                long beginningOfActivity = FilerIO.readLong(filer, "begin");
                filer.seek(beginningOfActivity);
                rawActivity = FilerIO.readByteArray(filer, "activity");
            }
            return objectMapper.readValue(rawActivity, MiruActivity.class);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public int lastId() {
        // this potentially includes null elements at the end of the array, but this index is immutable so it's safe to consider all indexes "written"
        return capacity();
    }

    @Override
    public void set(int index, MiruActivity activity) {
        throw new UnsupportedOperationException("On disk indexes are readOnly");
    }

    @Override
    public long sizeInMemory() throws Exception {
        return 0;
    }

    @Override
    public long sizeOnDisk() throws Exception {
        return filer.length();
    }

    private int capacity() {
        try {
            int size = indexSize.get();
            if (size == 0) {
                synchronized (filer.lock()) {
                    filer.seek(0);
                    size = FilerIO.readInt(filer, "size");
                }
                indexSize.set(size);
            }
            return size;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void close() {
        try {
            filer.close();
        } catch (IOException e) {
            log.error("Failed to remove activity index", e);
        }
    }

    @Override
    public void bulkImport(BulkExport<MiruActivity[]> bulkExport) throws Exception {
        //TODO this should ignore null elements in the tail to save space
        MiruActivity[] importActivities = bulkExport.bulkExport();
        int indexSize = importActivities.length;

        long length;
        try (RandomAccessFiler filer = new RandomAccessFiler(filerProvider.getBackingFile(), "rw")) {
            synchronized (filer.lock()) {
                filer.seek(0);
                FilerIO.writeInt(filer, indexSize, "size");

                long[] activityLocations = new long[indexSize];

                // First, skip to where activity starts and start streaming the activities in
                filer.seek(4 + (indexSize * 8));
                for (int i = 0; i < indexSize; i++) {
                    MiruActivity miruActivity = importActivities[i];
                    byte[] miruActivityBytes = objectMapper.writeValueAsBytes(miruActivity);

                    // Record the current location of this activity for backfilling
                    activityLocations[i] = filer.getFilePointer();

                    // Write out the activity
                    FilerIO.writeByteArray(filer, miruActivityBytes, "activity");
                }

                // Next, backfill the locations of each activity
                filer.seek(4);
                for (int i = 0; i < activityLocations.length; i++) {
                    FilerIO.writeLong(filer, activityLocations[i], "begin");
                }

                length = filer.length();
            }
        }

        this.filer = filerProvider.getFiler(length);
    }
}
