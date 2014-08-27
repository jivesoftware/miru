package com.jivesoftware.os.miru.service.index.memory;

import com.jivesoftware.os.jive.utils.chunk.store.MultiChunkStore;
import com.jivesoftware.os.jive.utils.io.FilerIO;
import com.jivesoftware.os.jive.utils.keyed.store.FileBackedKeyedStore;
import com.jivesoftware.os.jive.utils.keyed.store.SwappableFiler;
import com.jivesoftware.os.jive.utils.keyed.store.SwappingFiler;
import com.jivesoftware.os.jive.utils.logger.MetricLogger;
import com.jivesoftware.os.jive.utils.logger.MetricLoggerFactory;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.api.base.MiruTermId;
import com.jivesoftware.os.miru.service.activity.MiruInternalActivity;
import com.jivesoftware.os.miru.service.index.BulkExport;
import com.jivesoftware.os.miru.service.index.BulkImport;
import com.jivesoftware.os.miru.service.index.MiruActivityIndex;
import com.jivesoftware.os.miru.service.index.MiruInternalActivityMarshaller;
import java.io.File;
import java.util.concurrent.atomic.AtomicInteger;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * Short-lived (transient) impl. Like the mem-mapped impl, activity data is mem-mapped. However, set() is supported. The last index is only held in memory (not
 * stored on disk).
 */
public class MiruHybridActivityIndex implements MiruActivityIndex, BulkImport<MiruInternalActivity[]>, BulkExport<MiruInternalActivity[]> {

    private static final MetricLogger log = MetricLoggerFactory.getLogger();

    private final FileBackedKeyedStore keyedStore;
    private final AtomicInteger indexSize = new AtomicInteger();
    private final MiruInternalActivityMarshaller internalActivityMarshaller;

    public MiruHybridActivityIndex(File mapDirectory, File swapDirectory, MultiChunkStore chunkStore,
            MiruInternalActivityMarshaller internalActivityMarshaller) throws Exception {
        this.keyedStore = new FileBackedKeyedStore(mapDirectory.getAbsolutePath(), swapDirectory.getAbsolutePath(), 4, 100, chunkStore, 512);
        this.internalActivityMarshaller = internalActivityMarshaller;
    }

    @Override
    public MiruInternalActivity get(MiruTenantId tenantId, int index) {
        int capacity = indexSize.get();
        checkArgument(index >= 0 && index < capacity, "Index parameter is out of bounds. The value " + index + " must be >=0 and <" + capacity);
        try {
            SwappableFiler swappableFiler = keyedStore.get(FilerIO.intBytes(index), false);
            if (swappableFiler != null) {
                synchronized (swappableFiler.lock()) {
                    swappableFiler.sync();
                    swappableFiler.seek(0);
                    return internalActivityMarshaller.fromFiler(tenantId, swappableFiler);
                }
            }
            return null;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public MiruTermId[] get(MiruTenantId tenantId, int index, int fieldId) {
        int capacity = indexSize.get();
        checkArgument(index >= 0 && index < capacity, "Index parameter is out of bounds. The value " + index + " must be >=0 and <" + capacity);
        try {
            SwappableFiler swappableFiler = keyedStore.get(FilerIO.intBytes(index), false);
            if (swappableFiler != null) {
                synchronized (swappableFiler.lock()) {
                    swappableFiler.sync();
                    swappableFiler.seek(0);
                    return internalActivityMarshaller.fieldValueFromFiler(swappableFiler, fieldId);
                }
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
            //byte[] bytes = objectMapper.writeValueAsBytes(activity);
            byte[] bytes = internalActivityMarshaller.toBytes(activity);
            SwappableFiler swappableFiler = keyedStore.get(FilerIO.intBytes(index), true);
            synchronized (swappableFiler.lock()) {
                SwappingFiler swappingFiler = swappableFiler.swap(4 + bytes.length);
                //FilerIO.writeByteArray(swappingFiler, bytes, "activity");
                FilerIO.write(swappingFiler, bytes);
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
    public void bulkImport(MiruTenantId tenantId, BulkExport<MiruInternalActivity[]> bulkExport) throws Exception {
        MiruInternalActivity[] importActivities = bulkExport.bulkExport(tenantId);

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
    public MiruInternalActivity[] bulkExport(MiruTenantId tenantId) throws Exception {
        int capacity = indexSize.get();

        //TODO all activities need to fit in memory... sigh.
        //TODO need to "stream" this export/import.
        MiruInternalActivity[] activities = new MiruInternalActivity[capacity];
        for (int i = 0; i < activities.length; i++) {
            activities[i] = get(tenantId, i);
        }
        return activities;
    }
}
