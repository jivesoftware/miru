package com.jivesoftware.os.miru.service.index.memory;

import com.google.common.base.Optional;
import com.jivesoftware.os.jive.utils.chunk.store.MultiChunkStore;
import com.jivesoftware.os.jive.utils.io.Filer;
import com.jivesoftware.os.jive.utils.io.FilerIO;
import com.jivesoftware.os.jive.utils.keyed.store.FileBackedKeyedStore;
import com.jivesoftware.os.jive.utils.keyed.store.SwappableFiler;
import com.jivesoftware.os.jive.utils.keyed.store.SwappingFiler;
import com.jivesoftware.os.jive.utils.logger.MetricLogger;
import com.jivesoftware.os.jive.utils.logger.MetricLoggerFactory;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.api.base.MiruTermId;
import com.jivesoftware.os.miru.plugin.index.MiruActivityAndId;
import com.jivesoftware.os.miru.plugin.index.MiruActivityIndex;
import com.jivesoftware.os.miru.plugin.index.MiruInternalActivity;
import com.jivesoftware.os.miru.service.index.BulkExport;
import com.jivesoftware.os.miru.service.index.BulkImport;
import com.jivesoftware.os.miru.service.index.MiruInternalActivityMarshaller;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * Mem-mapped impl. Activity data lives in a keyed store, last index is an atomic integer optionally backed by a filer.
 * Since the optional filer is backed by disk, it's recommended that set() only be used without a filer (transient index).
 */
public class MiruHybridActivityIndex implements MiruActivityIndex, BulkImport<Iterator<MiruInternalActivity>>, BulkExport<Iterator<MiruInternalActivity>> {

    private static final MetricLogger log = MetricLoggerFactory.getLogger();

    private final FileBackedKeyedStore keyedStore;
    private final AtomicInteger indexSize = new AtomicInteger(-1);
    private final MiruInternalActivityMarshaller internalActivityMarshaller;
    private final Optional<Filer> indexSizeFiler;

    public MiruHybridActivityIndex(File mapDirectory, File swapDirectory, MultiChunkStore chunkStore,
        MiruInternalActivityMarshaller internalActivityMarshaller, Optional<Filer> indexSizeFiler) throws Exception {
        this.keyedStore = new FileBackedKeyedStore(mapDirectory.getAbsolutePath(), swapDirectory.getAbsolutePath(), 4, 100, chunkStore, 512);
        this.internalActivityMarshaller = internalActivityMarshaller;
        this.indexSizeFiler = indexSizeFiler;
    }

    @Override
    public MiruInternalActivity get(MiruTenantId tenantId, int index) {
        int capacity = capacity();
        checkArgument(index >= 0 && index < capacity, "Index parameter is out of bounds. The value %s must be >=0 and <%s", index, capacity);
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
        int capacity = capacity();
        checkArgument(index >= 0 && index < capacity, "Index parameter is out of bounds. The value %s must be >=0 and <%s", index, capacity);
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
        return capacity() - 1;
    }

    @Override
    public void set(List<MiruActivityAndId<MiruInternalActivity>> activityAndIds) {
        set(activityAndIds, true);
    }

    private void set(List<MiruActivityAndId<MiruInternalActivity>> activityAndIds, boolean checkCapacity) {
        for (MiruActivityAndId<MiruInternalActivity> activityAndId : activityAndIds) {
            int index = activityAndId.id;
            MiruInternalActivity activity = activityAndId.activity;
            checkArgument(index >= 0, "Index parameter is out of bounds. The value %s must be >=0", index);
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
                if (checkCapacity) {
                    checkCapacity(index);
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

    }

    @Override
    public long sizeInMemory() {
        return 0;
    }

    @Override
    public long sizeOnDisk() throws Exception {
        return (indexSizeFiler.isPresent() ? indexSizeFiler.get().length() : 0) + keyedStore.mapStoreSizeInBytes();
    }

    private int capacity() {
        try {
            int size = indexSize.get();
            if (size < 0) {
                if (indexSizeFiler.isPresent()) {
                    Filer filer = indexSizeFiler.get();
                    synchronized (filer.lock()) {
                        filer.seek(0);
                        size = FilerIO.readInt(filer, "size");
                    }
                } else {
                    size = 0;
                }
                indexSize.set(size);
            }
            return size;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private void checkCapacity(int index) throws IOException {
        log.trace("Check if index {} should extend capacity {}", index, indexSize);
        int size = index + 1;
        synchronized (indexSize) {
            if (size > indexSize.get()) {
                if (indexSizeFiler.isPresent()) {
                    Filer filer = indexSizeFiler.get();
                    synchronized (filer.lock()) {
                        filer.seek(0);
                        FilerIO.writeInt(filer, size, "size");
                    }
                }
                log.debug("Capacity extended to {}", size);
                indexSize.set(size);
            }
        }
    }

    @Override
    public void close() {
    }

    @Override
    public void bulkImport(MiruTenantId tenantId, BulkExport<Iterator<MiruInternalActivity>> bulkExport) throws Exception {
        Iterator<MiruInternalActivity> importActivities = bulkExport.bulkExport(tenantId);
        int batchSize = 1_000; //TODO expose to config

        List<MiruActivityAndId<MiruInternalActivity>> batch = new ArrayList<>(batchSize);
        int index = 0;
        while (importActivities.hasNext()) {
            MiruInternalActivity activity = importActivities.next();
            if (activity == null) {
                break;
            }
            batch.add(new MiruActivityAndId<>(activity, index));
            index++;
            if (batch.size() >= batchSize) {
                set(batch, false);
                batch.clear();
            }
        }

        if (!batch.isEmpty()) {
            set(batch, false);
        }

        checkCapacity(index - 1);
    }

    @Override
    public Iterator<MiruInternalActivity> bulkExport(final MiruTenantId tenantId) throws Exception {
        final int capacity = capacity();

        return new Iterator<MiruInternalActivity>() {
            private int index = 0;
            private MiruInternalActivity next;

            private void loadNext() {
                if (next == null && index < capacity) {
                    next = get(tenantId, index);
                }
            }

            @Override
            public boolean hasNext() {
                loadNext();
                return (next != null);
            }

            @Override
            public MiruInternalActivity next() {
                loadNext();
                MiruInternalActivity result = next;
                index++;
                next = null;
                return result;
            }

            @Override
            public void remove() {
                throw new UnsupportedOperationException();
            }
        };
    }
}
