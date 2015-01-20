package com.jivesoftware.os.miru.service.stream.allocator;

import com.jivesoftware.os.filer.chunk.store.ChunkStore;
import com.jivesoftware.os.filer.chunk.store.ChunkStoreInitializer;
import com.jivesoftware.os.filer.io.ByteBufferFactory;
import com.jivesoftware.os.jive.utils.logger.MetricLogger;
import com.jivesoftware.os.jive.utils.logger.MetricLoggerFactory;
import com.jivesoftware.os.miru.api.MiruPartitionCoord;
import java.io.IOException;

/**
 *
 */
public class HybridChunkAllocator implements MiruChunkAllocator {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    private final ByteBufferFactory rebuildByteBufferFactory;
    private final ByteBufferFactory cacheByteBufferFactory;
    private final long initialChunkSize;
    private final int numberOfChunkStores;
    private final boolean partitionDeleteChunkStoreOnClose;

    public HybridChunkAllocator(ByteBufferFactory rebuildByteBufferFactory,
        ByteBufferFactory cacheByteBufferFactory,
        long initialChunkSize,
        int numberOfChunkStores,
        boolean partitionDeleteChunkStoreOnClose) {
        this.rebuildByteBufferFactory = rebuildByteBufferFactory;
        this.cacheByteBufferFactory = cacheByteBufferFactory;
        this.initialChunkSize = initialChunkSize;
        this.numberOfChunkStores = numberOfChunkStores;
        this.partitionDeleteChunkStoreOnClose = partitionDeleteChunkStoreOnClose;
    }

    @Override
    public ChunkStore[] allocateChunkStores(MiruPartitionCoord coord) throws Exception {

        ChunkStore[] chunkStores = new ChunkStore[numberOfChunkStores];
        ChunkStoreInitializer chunkStoreInitializer = new ChunkStoreInitializer();
        for (int i = 0; i < numberOfChunkStores; i++) {
            chunkStores[i] = chunkStoreInitializer.create(rebuildByteBufferFactory,
                initialChunkSize,
                cacheByteBufferFactory,
                5_000); //TODO configure?
        }
        return chunkStores;
    }

    @Override
    public boolean checkExists(MiruPartitionCoord coord) throws Exception {
        return true;
    }

    public <BM> void close(ChunkStore[] chunkStores) {
        if (partitionDeleteChunkStoreOnClose) {
            for (ChunkStore chunkStore : chunkStores) {
                try {
                    chunkStore.delete();
                } catch (IOException e) {
                    LOG.warn("Failed to delete chunk store", e);
                }
            }
        }
    }
}
