package com.jivesoftware.os.miru.service.stream.allocator;

import com.jivesoftware.os.filer.chunk.store.ChunkStoreInitializer;
import com.jivesoftware.os.filer.io.ByteBufferFactory;
import com.jivesoftware.os.filer.io.api.StackBuffer;
import com.jivesoftware.os.filer.io.chunk.ChunkStore;
import com.jivesoftware.os.miru.api.MiruPartitionCoord;
import com.jivesoftware.os.miru.service.locator.MiruPartitionCoordIdentifier;
import com.jivesoftware.os.miru.service.locator.MiruResourceLocator;
import com.jivesoftware.os.miru.service.locator.MiruResourcePartitionIdentifier;
import com.jivesoftware.os.miru.service.stream.BerkeleyKeyedIndexStore;
import com.jivesoftware.os.miru.service.stream.KeyedIndexStore;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import com.sleepycat.je.EnvironmentConfig;
import java.io.File;
import java.io.IOException;

/**
 *
 */
public class OnDiskChunkAllocator implements MiruChunkAllocator {

    private static final MetricLogger log = MetricLoggerFactory.getLogger();

    private final MiruResourceLocator resourceLocator;
    private final ByteBufferFactory cacheByteBufferFactory;
    private final int numberOfChunkStores;
    private final int numberOfIndexStores;
    private final int partitionInitialChunkCacheSize;
    private final int partitionMaxChunkCacheSize;

    public OnDiskChunkAllocator(
        MiruResourceLocator resourceLocator,
        ByteBufferFactory cacheByteBufferFactory,
        int numberOfChunkStores,
        int numberOfIndexStores,
        int partitionInitialChunkCacheSize,
        int partitionMaxChunkCacheSize) {
        this.resourceLocator = resourceLocator;
        this.cacheByteBufferFactory = cacheByteBufferFactory;
        this.numberOfChunkStores = numberOfChunkStores;
        this.numberOfIndexStores = numberOfIndexStores;
        this.partitionInitialChunkCacheSize = partitionInitialChunkCacheSize;
        this.partitionMaxChunkCacheSize = partitionMaxChunkCacheSize;
    }

    @Override
    public boolean checkExists(MiruPartitionCoord coord) throws Exception {
        MiruPartitionCoordIdentifier identifier = new MiruPartitionCoordIdentifier(coord);
        File[] chunkDirs = resourceLocator.getPartitionSubDirectories(identifier, "chunks");
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
    public ChunkStore[] allocateChunkStores(MiruPartitionCoord coord, StackBuffer stackBuffer) throws Exception {

        MiruResourcePartitionIdentifier identifier = new MiruPartitionCoordIdentifier(coord);

        File[] subDirs = resourceLocator.getPartitionSubDirectories(identifier, "chunks");
        ChunkStore[] chunkStores = new ChunkStore[numberOfChunkStores];
        ChunkStoreInitializer chunkStoreInitializer = new ChunkStoreInitializer();
        for (int i = 0; i < numberOfChunkStores; i++) {
            int directoryOffset = Math.abs((coord.hashCode() + i) % subDirs.length);
            chunkStores[i] = chunkStoreInitializer.openOrCreate(
                subDirs,
                directoryOffset,
                "chunk-" + i,
                resourceLocator.getOnDiskInitialChunkSize(),
                cacheByteBufferFactory,
                partitionInitialChunkCacheSize,
                partitionMaxChunkCacheSize,
                stackBuffer);
        }
        return chunkStores;
    }

    @Override
    public KeyedIndexStore[] allocateIndexStores(MiruPartitionCoord coord) throws IOException {
        MiruResourcePartitionIdentifier identifier = new MiruPartitionCoordIdentifier(coord);
        EnvironmentConfig envConfig = new EnvironmentConfig()
            //.setConfigParam(EnvironmentConfig.ENV_RUN_CHECKPOINTER, "false")
            .setAllowCreate(true)
            .setSharedCache(true);
        envConfig.setCachePercentVoid(30);
        File[] subDirs = resourceLocator.getPartitionSubDirectories(identifier, "env");
        KeyedIndexStore[] indexStores = new KeyedIndexStore[numberOfIndexStores];
        for (int i = 0; i < numberOfIndexStores; i++) {
            int directoryOffset = Math.abs((coord.hashCode() + i) % subDirs.length);
            indexStores[i] = new BerkeleyKeyedIndexStore(subDirs, directoryOffset, "env-" + i, envConfig);
        }
        return indexStores;
    }

    @Override
    public void close(ChunkStore[] chunkStores, KeyedIndexStore[] indexStores) {
        for (KeyedIndexStore indexStore : indexStores) {
            indexStore.flush(false); //TODO config fsync
            indexStore.close();
        }
    }

}
