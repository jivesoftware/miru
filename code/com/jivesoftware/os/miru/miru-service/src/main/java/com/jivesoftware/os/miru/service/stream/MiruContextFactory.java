package com.jivesoftware.os.miru.service.stream;

import com.jivesoftware.os.filer.io.Filer;
import com.jivesoftware.os.filer.io.FilerIO;
import com.jivesoftware.os.filer.io.RandomAccessFiler;
import com.jivesoftware.os.jive.utils.logger.MetricLogger;
import com.jivesoftware.os.jive.utils.logger.MetricLoggerFactory;
import com.jivesoftware.os.miru.api.MiruBackingStorage;
import com.jivesoftware.os.miru.api.MiruPartitionCoord;
import com.jivesoftware.os.miru.api.MiruPartitionState;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.plugin.bitmap.MiruBitmaps;
import com.jivesoftware.os.miru.plugin.schema.MiruSchemaUnvailableException;
import com.jivesoftware.os.miru.service.index.BulkExport;
import com.jivesoftware.os.miru.service.index.BulkImport;
import com.jivesoftware.os.miru.service.locator.MiruHybridResourceLocator;
import com.jivesoftware.os.miru.service.locator.MiruPartitionCoordIdentifier;
import com.jivesoftware.os.miru.service.locator.MiruResourceLocator;
import com.jivesoftware.os.miru.service.locator.MiruResourcePartitionIdentifier;
import com.jivesoftware.os.miru.service.stream.allocator.MiruContextAllocator;
import java.io.File;
import java.io.IOException;
import java.util.Map;

/**
 * @author jonathan
 */
public class MiruContextFactory {

    private static MetricLogger log = MetricLoggerFactory.getLogger();

    private final Map<MiruBackingStorage, MiruContextAllocator> allocators;
    private final MiruResourceLocator diskResourceLocator;
    private final MiruHybridResourceLocator hybridResourceLocator;
    private final MiruBackingStorage defaultStorage;

    public MiruContextFactory(Map<MiruBackingStorage, MiruContextAllocator> allocators,
        MiruResourceLocator diskResourceLocator,
        MiruHybridResourceLocator transientResourceLocator,
        MiruBackingStorage defaultStorage) {
        this.allocators = allocators;
        this.diskResourceLocator = diskResourceLocator;
        this.hybridResourceLocator = transientResourceLocator;
        this.defaultStorage = defaultStorage;
    }

    public MiruBackingStorage findBackingStorage(MiruPartitionCoord coord) throws Exception {
        try {
            for (MiruBackingStorage storage : MiruBackingStorage.values()) {
                if (checkMarkedStorage(coord, storage)) {
                    return storage;
                }
            }
        } catch (MiruSchemaUnvailableException e) {
            log.warn("Schema not registered for tenant {}, using default storage", coord.tenantId);
        }
        return defaultStorage;
    }

    private MiruContextAllocator getAllocator(MiruBackingStorage storage) {
        MiruContextAllocator allocator = allocators.get(storage);
        if (allocator != null) {
            return allocator;
        } else {
            throw new RuntimeException("backingStorage:" + storage + " is unsupported.");
        }
    }

    public <BM> MiruContext<BM> allocate(MiruBitmaps<BM> bitmaps, MiruPartitionCoord coord, MiruBackingStorage storage) throws Exception {
        return getAllocator(storage).allocate(bitmaps, coord);
    }

    public <BM> MiruContext<BM> copy(MiruBitmaps<BM> bitmaps, MiruPartitionCoord coord, MiruContext<BM> from, MiruBackingStorage toStorage) throws Exception {
        return copy(coord.tenantId, from, getAllocator(toStorage).allocate(bitmaps, coord));
    }

    public <BM> MiruContext<BM> stateChanged(MiruBitmaps<BM> bitmaps,
        MiruPartitionCoord coord,
        MiruContext<BM> from,
        MiruBackingStorage fromStorage,
        MiruPartitionState state)
        throws Exception {
        return getAllocator(fromStorage).stateChanged(bitmaps, coord, from, state);
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    private <BM> MiruContext<BM> copy(MiruTenantId tenantId, MiruContext<BM> from, MiruContext<BM> to) throws Exception {
        ((BulkImport) to.timeIndex).bulkImport(tenantId, (BulkExport) from.timeIndex);
        ((BulkImport) to.activityIndex).bulkImport(tenantId, (BulkExport) from.activityIndex);
        ((BulkImport) to.fieldIndex).bulkImport(tenantId, (BulkExport) from.fieldIndex);
        ((BulkImport) to.authzIndex).bulkImport(tenantId, (BulkExport) from.authzIndex);
        ((BulkImport) to.removalIndex).bulkImport(tenantId, (BulkExport) from.removalIndex);
        ((BulkImport) to.unreadTrackingIndex).bulkImport(tenantId, (BulkExport) from.unreadTrackingIndex);
        ((BulkImport) to.inboxIndex).bulkImport(tenantId, (BulkExport) from.inboxIndex);
        return to;
    }

    public void markStorage(MiruPartitionCoord coord, MiruBackingStorage marked) throws Exception {
        MiruResourcePartitionIdentifier identifier = new MiruPartitionCoordIdentifier(coord);

        for (MiruBackingStorage storage : MiruBackingStorage.values()) {
            if (storage != marked) {
                diskResourceLocator.getFilerFile(identifier, storage.name()).delete();
            }
        }

        diskResourceLocator.getFilerFile(identifier, marked.name()).createNewFile();
    }

    private boolean checkMarkedStorage(MiruPartitionCoord coord, MiruBackingStorage storage) throws Exception {
        File file = diskResourceLocator.getFilerFile(new MiruPartitionCoordIdentifier(coord), storage.name());
        return file.exists() && getAllocator(storage).checkMarkedStorage(coord);
    }

    public void markSip(MiruPartitionCoord coord, long sipTimestamp) throws Exception {
        File file = diskResourceLocator.getFilerFile(new MiruPartitionCoordIdentifier(coord), "sip");
        try (Filer filer = new RandomAccessFiler(file, "rw")) {
            filer.setLength(0);
            filer.seek(0);
            FilerIO.writeLong(filer, sipTimestamp, "sip");
        }
    }

    public long getSip(MiruPartitionCoord coord) throws Exception {
        File file = diskResourceLocator.getFilerFile(new MiruPartitionCoordIdentifier(coord), "sip");
        if (file.exists()) {
            try (Filer filer = new RandomAccessFiler(file, "rw")) {
                filer.seek(0);
                return FilerIO.readLong(filer, "sip");
            }
        } else {
            return 0;
        }
    }

    public void cleanDisk(MiruPartitionCoord coord) throws IOException {
        diskResourceLocator.clean(new MiruPartitionCoordIdentifier(coord));
    }

    public <BM> void close(MiruContext<BM> context, MiruBackingStorage storage) {
        context.activityIndex.close();
        context.authzIndex.close();
        context.timeIndex.close();
        context.unreadTrackingIndex.close();
        context.inboxIndex.close();

        if (context.transientResource.isPresent()) {
            hybridResourceLocator.release(context.transientResource.get());
        }

        getAllocator(storage).close(context);
    }

}
