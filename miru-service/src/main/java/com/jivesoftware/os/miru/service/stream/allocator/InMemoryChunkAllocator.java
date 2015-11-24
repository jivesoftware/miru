package com.jivesoftware.os.miru.service.stream.allocator;

import com.jivesoftware.os.filer.chunk.store.ChunkStoreInitializer;
import com.jivesoftware.os.filer.io.ByteBufferFactory;
import com.jivesoftware.os.filer.io.api.StackBuffer;
import com.jivesoftware.os.filer.io.chunk.ChunkStore;
import com.jivesoftware.os.miru.api.MiruPartitionCoord;
import com.jivesoftware.os.miru.service.stream.KeyedIndexStore;
import com.jivesoftware.os.miru.service.stream.MemoryKeyedIndexStore;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import java.io.IOException;

/**
 *
 */
public class InMemoryChunkAllocator implements MiruChunkAllocator {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    private final ByteBufferFactory rebuildByteBufferFactory;
    private final ByteBufferFactory cacheByteBufferFactory;
    private final long initialChunkSize;
    private final int numberOfChunkStores;
    private final int numberOfIndexStores;
    private final boolean partitionDeleteChunkStoreOnClose;
    private final boolean partitionDeleteIndexStoreOnClose;
    private final int partitionInitialChunkCacheSize;
    private final int partitionMaxChunkCacheSize;

    public InMemoryChunkAllocator(ByteBufferFactory rebuildByteBufferFactory,
        ByteBufferFactory cacheByteBufferFactory,
        long initialChunkSize,
        int numberOfChunkStores,
        int numberOfIndexStores,
        boolean partitionDeleteChunkStoreOnClose,
        boolean partitionDeleteIndexStoreOnClose,
        int partitionInitialChunkCacheSize,
        int partitionMaxChunkCacheSize) {
        this.rebuildByteBufferFactory = rebuildByteBufferFactory;
        this.cacheByteBufferFactory = cacheByteBufferFactory;
        this.initialChunkSize = initialChunkSize;
        this.numberOfChunkStores = numberOfChunkStores;
        this.numberOfIndexStores = numberOfIndexStores;
        this.partitionDeleteChunkStoreOnClose = partitionDeleteChunkStoreOnClose;
        this.partitionDeleteIndexStoreOnClose = partitionDeleteIndexStoreOnClose;
        this.partitionInitialChunkCacheSize = partitionInitialChunkCacheSize;
        this.partitionMaxChunkCacheSize = partitionMaxChunkCacheSize;
    }

    @Override
    public ChunkStore[] allocateChunkStores(MiruPartitionCoord coord, StackBuffer stackBuffer) throws Exception {

        ChunkStore[] chunkStores = new ChunkStore[numberOfChunkStores];
        ChunkStoreInitializer chunkStoreInitializer = new ChunkStoreInitializer();
        for (int i = 0; i < numberOfChunkStores; i++) {
            chunkStores[i] = chunkStoreInitializer.create(rebuildByteBufferFactory,
                initialChunkSize,
                cacheByteBufferFactory,
                partitionInitialChunkCacheSize,
                partitionMaxChunkCacheSize,
                stackBuffer);
        }
        return chunkStores;
    }

    @Override
    public KeyedIndexStore[] allocateIndexStores(MiruPartitionCoord coord) {
        KeyedIndexStore[] indexStores = new KeyedIndexStore[numberOfIndexStores];
        for (int i = 0; i < numberOfIndexStores; i++) {
            indexStores[i] = new MemoryKeyedIndexStore();
        }
        return indexStores;
    }

    @Override
    public boolean checkExists(MiruPartitionCoord coord) throws Exception {
        return true;
    }

    public void close(ChunkStore[] chunkStores, KeyedIndexStore[] indexStores) throws IOException {
        if (partitionDeleteChunkStoreOnClose) {
            for (ChunkStore chunkStore : chunkStores) {
                try {
                    chunkStore.delete();
                } catch (IOException e) {
                    LOG.warn("Failed to delete chunk store", e);
                }
            }
        }

        for (KeyedIndexStore indexStore : indexStores) {
            indexStore.close();
            if (partitionDeleteIndexStoreOnClose) {
                indexStore.delete();
            }
        }
    }
}
