package com.jivesoftware.os.miru.service.stream;

import com.google.common.base.Optional;
import com.jivesoftware.os.miru.service.index.BulkExport;
import com.jivesoftware.os.miru.service.index.BulkImport;
import com.jivesoftware.os.miru.service.index.MiruTimeIndex;
import com.jivesoftware.os.miru.service.stream.locator.MiruResourcePartitionIdentifier;
import com.jivesoftware.os.jive.utils.chunk.store.ChunkStore;
import java.util.Map;

/**
 * Composes the building blocks of a MiruStream together for convenience.
 *
 * @author jonathan
 */
public class MiruStream {

    private final MiruIndexStream indexStream;
    private final MiruQueryStream queryStream;
    private final MiruReadTrackStream readTrackStream;
    private final MiruTimeIndex timeIndex;
    private final Optional<ChunkStore> chunkStore;

    private Map<String, BulkExport<?>> exportHandles;
    private Map<String, BulkImport<?>> importHandles;
    private Optional<? extends MiruResourcePartitionIdentifier> transientResource;

    public MiruStream(MiruIndexStream indexStream, MiruQueryStream queryStream, MiruReadTrackStream readTrackStream, MiruTimeIndex timeIndex,
        Optional<ChunkStore> chunkStore) {
        this.indexStream = indexStream;
        this.queryStream = queryStream;
        this.readTrackStream = readTrackStream;
        this.timeIndex = timeIndex;
        this.chunkStore = chunkStore;
        this.transientResource = Optional.absent();
    }

    public MiruStream exportable(Map<String, BulkExport<?>> exportHandles) {
        this.exportHandles = exportHandles;
        return this;
    }

    public MiruStream importable(Map<String, BulkImport<?>> importHandles) {
        this.importHandles = importHandles;
        return this;
    }

    public <T extends MiruResourcePartitionIdentifier> MiruStream withTransientResource(T identifier) {
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

    public MiruIndexStream getIndexStream() {
        return indexStream;
    }

    public MiruQueryStream getQueryStream() {
        return queryStream;
    }

    public MiruReadTrackStream getReadTrackStream() {
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
