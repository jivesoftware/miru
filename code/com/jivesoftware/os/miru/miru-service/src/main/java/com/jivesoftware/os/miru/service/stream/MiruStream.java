package com.jivesoftware.os.miru.service.stream;

import com.google.common.base.Optional;
import com.jivesoftware.os.jive.utils.chunk.store.MultiChunkStore;
import com.jivesoftware.os.miru.service.index.BulkExport;
import com.jivesoftware.os.miru.service.index.BulkImport;
import com.jivesoftware.os.miru.service.index.MiruTimeIndex;
import com.jivesoftware.os.miru.service.stream.locator.MiruResourcePartitionIdentifier;
import java.util.Map;

/**
 * Composes the building blocks of a MiruStream together for convenience.
 *
 * @author jonathan
 */
public class MiruStream<BM> {

    private final MiruIndexStream <BM>indexStream;
    private final MiruQueryStream<BM> queryStream;
    private final MiruReadTrackStream<BM> readTrackStream;
    private final MiruTimeIndex timeIndex;
    private final Optional<MultiChunkStore> chunkStore;

    private Map<String, BulkExport<?>> exportHandles;
    private Map<String, BulkImport<?>> importHandles;
    private Optional<? extends MiruResourcePartitionIdentifier> transientResource;

    public MiruStream(MiruIndexStream<BM> indexStream,
            MiruQueryStream<BM> queryStream,
            MiruReadTrackStream<BM> readTrackStream,
            MiruTimeIndex timeIndex,
            Optional<MultiChunkStore> chunkStore) {
        this.indexStream = indexStream;
        this.queryStream = queryStream;
        this.readTrackStream = readTrackStream;
        this.timeIndex = timeIndex;
        this.chunkStore = chunkStore;
        this.transientResource = Optional.absent();
    }

    public MiruStream<BM> exportable(Map<String, BulkExport<?>> exportHandles) {
        this.exportHandles = exportHandles;
        return this;
    }

    public MiruStream<BM> importable(Map<String, BulkImport<?>> importHandles) {
        this.importHandles = importHandles;
        return this;
    }

    public <T extends MiruResourcePartitionIdentifier> MiruStream<BM> withTransientResource(T identifier) {
        this.transientResource = Optional.of(identifier);
        return this;
    }

    public long sizeInMemory() throws Exception {
        long sizeInBytes = 0;
        sizeInBytes += queryStream.activityIndex.sizeInMemory();
        sizeInBytes += queryStream.authzIndex.sizeInMemory();
        sizeInBytes += queryStream.fieldIndex.sizeInMemory();
        sizeInBytes += queryStream.inboxIndex.sizeInMemory();
        sizeInBytes += queryStream.removalIndex.sizeInMemory();
        sizeInBytes += queryStream.timeIndex.sizeInMemory();
        sizeInBytes += queryStream.unreadTrackingIndex.sizeInMemory();
        return sizeInBytes;
    }

    public long sizeOnDisk() throws Exception {
        long sizeInBytes = 0;
        sizeInBytes += queryStream.activityIndex.sizeOnDisk();
        sizeInBytes += queryStream.authzIndex.sizeOnDisk();
        sizeInBytes += queryStream.fieldIndex.sizeOnDisk();
        sizeInBytes += queryStream.inboxIndex.sizeOnDisk();
        sizeInBytes += queryStream.removalIndex.sizeOnDisk();
        sizeInBytes += queryStream.timeIndex.sizeOnDisk();
        sizeInBytes += queryStream.unreadTrackingIndex.sizeOnDisk();
        if (chunkStore.isPresent()) {
            sizeInBytes += chunkStore.get().sizeInBytes();
        }
        return sizeInBytes;
    }

    public MiruIndexStream<BM> getIndexStream() {
        return indexStream;
    }

    public MiruQueryStream<BM> getQueryStream() {
        return queryStream;
    }

    public MiruReadTrackStream<BM> getReadTrackStream() {
        return readTrackStream;
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
