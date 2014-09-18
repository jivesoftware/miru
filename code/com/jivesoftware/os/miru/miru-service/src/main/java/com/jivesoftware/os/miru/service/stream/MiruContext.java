package com.jivesoftware.os.miru.service.stream;

import com.google.common.base.Optional;
import com.jivesoftware.os.jive.utils.chunk.store.MultiChunkStore;
import com.jivesoftware.os.miru.plugin.context.MiruReadTrackContext;
import com.jivesoftware.os.miru.plugin.context.MiruRequestContext;
import com.jivesoftware.os.miru.plugin.index.MiruTimeIndex;
import com.jivesoftware.os.miru.service.index.BulkExport;
import com.jivesoftware.os.miru.service.index.BulkImport;
import com.jivesoftware.os.miru.service.locator.MiruResourcePartitionIdentifier;
import java.util.Map;

/**
 * Composes the building blocks of a MiruContext together for convenience.
 *
 * @author jonathan
 */
public class MiruContext<BM> {

    private final MiruIndexContext<BM> indexContext;
    private final MiruRequestContext<BM> queryContext;
    private final MiruReadTrackContext<BM> readTrackContext;
    private final MiruTimeIndex timeIndex;
    private final Optional<MultiChunkStore> chunkStore;

    private Map<String, BulkExport<?>> exportHandles;
    private Map<String, BulkImport<?>> importHandles;
    private Optional<? extends MiruResourcePartitionIdentifier> transientResource;

    public MiruContext(MiruIndexContext<BM> indexContext,
            MiruRequestContext<BM> queryContext,
            MiruReadTrackContext<BM> readTrackContext,
            MiruTimeIndex timeIndex,
            Optional<MultiChunkStore> chunkStore) {
        this.indexContext = indexContext;
        this.queryContext = queryContext;
        this.readTrackContext = readTrackContext;
        this.timeIndex = timeIndex;
        this.chunkStore = chunkStore;
        this.transientResource = Optional.absent();
    }

    public MiruContext<BM> exportable(Map<String, BulkExport<?>> exportHandles) {
        this.exportHandles = exportHandles;
        return this;
    }

    public MiruContext<BM> importable(Map<String, BulkImport<?>> importHandles) {
        this.importHandles = importHandles;
        return this;
    }

    public <T extends MiruResourcePartitionIdentifier> MiruContext<BM> withTransientResource(T identifier) {
        this.transientResource = Optional.of(identifier);
        return this;
    }

    public long sizeInMemory() throws Exception {
        long sizeInBytes = 0;
        sizeInBytes += queryContext.activityIndex.sizeInMemory();
        sizeInBytes += queryContext.authzIndex.sizeInMemory();
        sizeInBytes += queryContext.fieldIndex.sizeInMemory();
        sizeInBytes += queryContext.inboxIndex.sizeInMemory();
        sizeInBytes += queryContext.removalIndex.sizeInMemory();
        sizeInBytes += queryContext.timeIndex.sizeInMemory();
        sizeInBytes += queryContext.unreadTrackingIndex.sizeInMemory();
        return sizeInBytes;
    }

    public long sizeOnDisk() throws Exception {
        long sizeInBytes = 0;
        sizeInBytes += queryContext.activityIndex.sizeOnDisk();
        sizeInBytes += queryContext.authzIndex.sizeOnDisk();
        sizeInBytes += queryContext.fieldIndex.sizeOnDisk();
        sizeInBytes += queryContext.inboxIndex.sizeOnDisk();
        sizeInBytes += queryContext.removalIndex.sizeOnDisk();
        sizeInBytes += queryContext.timeIndex.sizeOnDisk();
        sizeInBytes += queryContext.unreadTrackingIndex.sizeOnDisk();
        if (chunkStore.isPresent()) {
            sizeInBytes += chunkStore.get().sizeInBytes();
        }
        return sizeInBytes;
    }

    public MiruIndexContext<BM> getIndexContext() {
        return indexContext;
    }

    public MiruRequestContext<BM> getQueryContext() {
        return queryContext;
    }

    public MiruReadTrackContext<BM> getReadTrackContext() {
        return readTrackContext;
    }

    public MiruTimeIndex getTimeIndex() {
        return timeIndex;
    }

    public Map<String, BulkExport<?>> getExportHandles() {
        return exportHandles;
    }

    public Map<String, BulkImport<?>> getImportHandles() {
        return importHandles;
    }

    public Optional<? extends MiruResourcePartitionIdentifier> getTransientResource() {
        return transientResource;
    }
}
