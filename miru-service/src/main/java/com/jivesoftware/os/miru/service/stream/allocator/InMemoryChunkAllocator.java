package com.jivesoftware.os.miru.service.stream.allocator;

import com.google.common.collect.Maps;
import com.google.common.io.Files;
import com.jivesoftware.os.filer.chunk.store.ChunkStoreInitializer;
import com.jivesoftware.os.filer.io.ByteBufferFactory;
import com.jivesoftware.os.filer.io.api.StackBuffer;
import com.jivesoftware.os.filer.io.chunk.ChunkStore;
import com.jivesoftware.os.lab.LABEnvironment;
import com.jivesoftware.os.lab.LABValueMerger;
import com.jivesoftware.os.miru.api.MiruPartitionCoord;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ExecutorService;

/**
 *
 */
public class InMemoryChunkAllocator implements MiruChunkAllocator {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    private final ByteBufferFactory rebuildByteBufferFactory;
    private final ByteBufferFactory cacheByteBufferFactory;
    private final long initialChunkSize;
    private final int numberOfChunkStores;
    private final boolean partitionDeleteChunkStoreOnClose;
    private final int partitionInitialChunkCacheSize;
    private final int partitionMaxChunkCacheSize;

    private final ExecutorService buildLABCompactorThreadPool = LABEnvironment.buildLABCompactorThreadPool(12);
    private final ExecutorService buildLABDestroyThreadPool = LABEnvironment.buildLABDestroyThreadPool(12);

    public InMemoryChunkAllocator(ByteBufferFactory rebuildByteBufferFactory,
        ByteBufferFactory cacheByteBufferFactory,
        long initialChunkSize,
        int numberOfChunkStores,
        boolean partitionDeleteChunkStoreOnClose,
        int partitionInitialChunkCacheSize,
        int partitionMaxChunkCacheSize) {
        this.rebuildByteBufferFactory = rebuildByteBufferFactory;
        this.cacheByteBufferFactory = cacheByteBufferFactory;
        this.initialChunkSize = initialChunkSize;
        this.numberOfChunkStores = numberOfChunkStores;
        this.partitionDeleteChunkStoreOnClose = partitionDeleteChunkStoreOnClose;
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

    private final Map<MiruPartitionCoord, File> hack = Maps.newConcurrentMap();

    @Override
    public File[] getLabDirs(MiruPartitionCoord coord) throws Exception {

        return new File[]{hack.computeIfAbsent(coord, (MiruPartitionCoord t) -> {
            File dir = Files.createTempDir(); // Sorry
            dir.deleteOnExit();
            return dir;
        })};
    }

    @Override
    public LABEnvironment[] allocateLABEnvironments(File[] labDirs) throws Exception {

        LABEnvironment[] environments = new LABEnvironment[labDirs.length];
        for (int i = 0; i < labDirs.length; i++) {
            environments[i] = new LABEnvironment(buildLABCompactorThreadPool,
                buildLABDestroyThreadPool,
                labDirs[i],
                new LABValueMerger(),
                true, 4, 16, 1024);

        }
        return environments;
    }

    @Override
    public boolean checkExists(MiruPartitionCoord coord) throws Exception {
        return true;
    }

    @Override
    public void close(ChunkStore[] chunkStores) {
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
