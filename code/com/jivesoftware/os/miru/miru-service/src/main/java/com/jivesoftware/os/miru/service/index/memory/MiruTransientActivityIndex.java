package com.jivesoftware.os.miru.service.index.memory;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.jivesoftware.os.jive.utils.chunk.store.ChunkStore;
import com.jivesoftware.os.jive.utils.io.FilerIO;
import com.jivesoftware.os.jive.utils.keyed.store.FileBackedKeyedStore;
import com.jivesoftware.os.jive.utils.keyed.store.SwappableFiler;
import com.jivesoftware.os.jive.utils.keyed.store.SwappingFiler;
import com.jivesoftware.os.jive.utils.logger.MetricLogger;
import com.jivesoftware.os.jive.utils.logger.MetricLoggerFactory;
import com.jivesoftware.os.miru.service.activity.MiruInternalActivity;
import com.jivesoftware.os.miru.service.index.BulkExport;
import com.jivesoftware.os.miru.service.index.BulkImport;
import com.jivesoftware.os.miru.service.index.MiruActivityIndex;
import java.io.File;
import java.util.concurrent.atomic.AtomicInteger;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * Short-lived (transient) impl. Like the mem-mapped impl, activity data is mem-mapped. However, set() is supported.
 * The last index is only held in memory (not stored on disk).
 */
public class MiruTransientActivityIndex implements MiruActivityIndex, BulkImport<MiruInternalActivity[]>, BulkExport<MiruInternalActivity[]> {

    private static final MetricLogger log = MetricLoggerFactory.getLogger();

    private final FileBackedKeyedStore keyedStore;
    private final ObjectMapper objectMapper;
    private final AtomicInteger indexSize = new AtomicInteger();

    public MiruTransientActivityIndex(File mapDirectory, File swapDirectory, ChunkStore chunkStore, ObjectMapper objectMapper) throws Exception {
        this.objectMapper = objectMapper;
        this.keyedStore = new FileBackedKeyedStore(mapDirectory.getAbsolutePath(), swapDirectory.getAbsolutePath(), 4, 100, chunkStore, 512);
    }

    @Override
    public MiruInternalActivity get(int index) {
        int capacity = indexSize.get();
        checkArgument(index >= 0 && index < capacity, "Index parameter is out of bounds. The value " + index + " must be >=0 and <" + capacity);
        try {
            SwappableFiler swappableFiler = keyedStore.get(FilerIO.intBytes(index), false);
            if (swappableFiler != null) {
                byte[] bytes;
                synchronized (swappableFiler.lock()) {
                    swappableFiler.sync();
                    swappableFiler.seek(0);
                    bytes = FilerIO.readByteArray(swappableFiler, "activity");
                }
                return objectMapper.readValue(bytes, MiruInternalActivity.class);
            }
            return null;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public int lastId() {
        return indexSize.get() - 1;
    }

    @Override
    public void set(int index, MiruInternalActivity activity) {
        checkArgument(index >= 0, "Index parameter is out of bounds. The value " + index + " must be >=0");
        try {
            byte[] bytes = objectMapper.writeValueAsBytes(activity);
            SwappableFiler swappableFiler = keyedStore.get(FilerIO.intBytes(index), true);
            synchronized (swappableFiler.lock()) {
                SwappingFiler swappingFiler = swappableFiler.swap(4 + bytes.length);
                FilerIO.writeByteArray(swappingFiler, bytes, "activity");
                swappingFiler.commit();
            }
            checkCapacity(index);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public long sizeInMemory() {
        return 0;
    }

    @Override
    public long sizeOnDisk() throws Exception {
        return keyedStore.mapStoreSizeInBytes();
    }

    private void checkCapacity(int index) {
        log.trace("Check if index {} should extend capacity {}", index, indexSize);
        int size = index + 1;
        synchronized (indexSize) {
            if (size > indexSize.get()) {
                log.debug("Capacity extended to {}", size);
                indexSize.set(size);
            }
        }
    }

    @Override
    public void close() {
    }

    @Override
    public void bulkImport(BulkExport<MiruInternalActivity[]> bulkExport) throws Exception {
        MiruInternalActivity[] importActivities = bulkExport.bulkExport();

        int lastIndex;
        for (lastIndex = importActivities.length - 1; lastIndex >= 0 && importActivities[lastIndex] == null; lastIndex--) {
            // walk to first non-null
        }

        for (int index = 0; index <= lastIndex; index++) {
            MiruInternalActivity activity = importActivities[index];
            if (activity != null) {
                set(index, activity);
            }
        }
    }

    @Override
    public MiruInternalActivity[] bulkExport() throws Exception {
        int capacity = indexSize.get();

        //TODO all activities need to fit in memory... sigh.
        //TODO need to "stream" this export/import.
        MiruInternalActivity[] activities = new MiruInternalActivity[capacity];
        for (int i = 0; i < activities.length; i++) {
            activities[i] = get(i);
        }
        return activities;
    }
}
