package com.jivesoftware.os.miru.service.stream.allocator;

import com.jivesoftware.os.filer.chunk.store.ChunkStoreInitializer;
import com.jivesoftware.os.filer.io.ByteBufferFactory;
import com.jivesoftware.os.filer.io.chunk.ChunkStore;
import com.jivesoftware.os.miru.api.MiruPartitionCoord;
import com.jivesoftware.os.miru.service.locator.MiruPartitionCoordIdentifier;
import com.jivesoftware.os.miru.service.locator.MiruResourceLocator;
import com.jivesoftware.os.miru.service.locator.MiruResourcePartitionIdentifier;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import java.io.File;

/**
 *
 */
public class OnDiskChunkAllocator implements MiruChunkAllocator {

    private static final MetricLogger log = MetricLoggerFactory.getLogger();

    private final MiruResourceLocator resourceLocator;
    private final ByteBufferFactory cacheByteBufferFactory;
    private final int numberOfChunkStores;
    private final int partitionInitialChunkCacheSize;
    private final int partitionMaxChunkCacheSize;

    public OnDiskChunkAllocator(
        MiruResourceLocator resourceLocator,
        ByteBufferFactory cacheByteBufferFactory,
        int numberOfChunkStores,
        int partitionInitialChunkCacheSize,
        int partitionMaxChunkCacheSize) {
        this.resourceLocator = resourceLocator;
        this.cacheByteBufferFactory = cacheByteBufferFactory;
        this.numberOfChunkStores = numberOfChunkStores;
        this.partitionInitialChunkCacheSize = partitionInitialChunkCacheSize;
        this.partitionMaxChunkCacheSize = partitionMaxChunkCacheSize;
    }

    @Override
    public boolean checkExists(MiruPartitionCoord coord) throws Exception {
        MiruPartitionCoordIdentifier identifier = new MiruPartitionCoordIdentifier(coord);
        File[] chunkDirs = resourceLocator.getChunkDirectories(identifier, "chunks");
        for (int i = 0; i < numberOfChunkStores; i++) {
            int directoryOffset = Math.abs((coord.hashCode() + i) % chunkDirs.length);
            if (!new ChunkStoreInitializer().checkExists(chunkDirs, directoryOffset, "chunk-" + i)) {
                log.warn("Partition missing chunk {} for {}", i, coord);
                return false;
            }
        }

        log.info("Partition is on disk for {}", coord);
        return true;
    }

    @Override
    public ChunkStore[] allocateChunkStores(MiruPartitionCoord coord, byte[] primitiveBuffer) throws Exception {

        MiruResourcePartitionIdentifier identifier = new MiruPartitionCoordIdentifier(coord);

        File[] chunkDirs = resourceLocator.getChunkDirectories(identifier, "chunks");
        ChunkStore[] chunkStores = new ChunkStore[numberOfChunkStores];
        ChunkStoreInitializer chunkStoreInitializer = new ChunkStoreInitializer();
        for (int i = 0; i < numberOfChunkStores; i++) {
            int directoryOffset = Math.abs((coord.hashCode() + i) % chunkDirs.length);
            chunkStores[i] = chunkStoreInitializer.openOrCreate(
                chunkDirs,
                directoryOffset,
                "chunk-" + i,
                resourceLocator.getOnDiskInitialChunkSize(),
                cacheByteBufferFactory,
                partitionInitialChunkCacheSize,
                partitionMaxChunkCacheSize,
                primitiveBuffer);
        }
        return chunkStores;
    }

    @Override
    public void close(ChunkStore[] chunkStores) {
    }

}
