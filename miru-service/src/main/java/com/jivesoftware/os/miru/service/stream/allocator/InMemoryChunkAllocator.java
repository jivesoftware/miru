package com.jivesoftware.os.miru.service.stream.allocator;

import com.google.common.collect.Lists;
import com.jivesoftware.os.filer.chunk.store.ChunkStoreInitializer;
import com.jivesoftware.os.filer.io.ByteBufferFactory;
import com.jivesoftware.os.filer.io.api.StackBuffer;
import com.jivesoftware.os.filer.io.chunk.ChunkStore;
import com.jivesoftware.os.lab.LABEnvironment;
import com.jivesoftware.os.miru.api.MiruPartitionCoord;
import com.jivesoftware.os.miru.service.locator.MiruResourceLocator;
import com.jivesoftware.os.miru.service.locator.MiruResourcePartitionIdentifier;
import com.jivesoftware.os.miru.service.locator.TransientPartitionCoordIdentifier;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.ExecutorService;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.builder.HashCodeBuilder;

/**
 *
 */
public class InMemoryChunkAllocator implements MiruChunkAllocator {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    private final MiruResourceLocator resourceLocator;
    private final ByteBufferFactory rebuildByteBufferFactory;
    private final ByteBufferFactory cacheByteBufferFactory;
    private final long initialChunkSize;
    private final int numberOfChunkStores;
    private final boolean partitionDeleteChunkStoreOnClose;
    private final int partitionInitialChunkCacheSize;
    private final int partitionMaxChunkCacheSize;
    private final boolean useLabIndexes;

    private final ExecutorService buildLABCompactorThreadPool = LABEnvironment.buildLABCompactorThreadPool(12);
    private final ExecutorService buildLABDestroyThreadPool = LABEnvironment.buildLABDestroyThreadPool(12);

    public InMemoryChunkAllocator(MiruResourceLocator resourceLocator,
        ByteBufferFactory rebuildByteBufferFactory,
        ByteBufferFactory cacheByteBufferFactory,
        long initialChunkSize,
        int numberOfChunkStores,
        boolean partitionDeleteChunkStoreOnClose,
        int partitionInitialChunkCacheSize,
        int partitionMaxChunkCacheSize,
        boolean useLabIndexes) {
        this.resourceLocator = resourceLocator;
        this.rebuildByteBufferFactory = rebuildByteBufferFactory;
        this.cacheByteBufferFactory = cacheByteBufferFactory;
        this.initialChunkSize = initialChunkSize;
        this.numberOfChunkStores = numberOfChunkStores;
        this.partitionDeleteChunkStoreOnClose = partitionDeleteChunkStoreOnClose;
        this.partitionInitialChunkCacheSize = partitionInitialChunkCacheSize;
        this.partitionMaxChunkCacheSize = partitionMaxChunkCacheSize;
        this.useLabIndexes = useLabIndexes;
    }

    @Override
    public ChunkStore[] allocateChunkStores(MiruPartitionCoord coord) throws Exception {

        ChunkStore[] chunkStores = new ChunkStore[numberOfChunkStores];
        ChunkStoreInitializer chunkStoreInitializer = new ChunkStoreInitializer();
        StackBuffer stackBuffer = new StackBuffer();
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
    public File[] getLabDirs(MiruPartitionCoord coord) throws Exception {
        MiruResourcePartitionIdentifier identifier = new TransientPartitionCoordIdentifier(coord);
        File[] baseDirs = resourceLocator.getChunkDirectories(identifier, "labs-transient");
        int hashCode = hash(coord);
        List<File> dirs = Lists.newArrayList();
        for (int i = 0; i < numberOfChunkStores; i++) {
            int directoryOffset = offset(hashCode, i, 0, baseDirs.length);
            dirs.add(new File(baseDirs[directoryOffset], "lab-" + i));
        }
        return dirs.toArray(new File[0]);
    }

    private static int offset(int hashCode, int index, int shift, int length) {
        long shifted = (hashCode & 0xFFFF) + index + shift;
        long offset = shifted % length;
        if (offset > Integer.MAX_VALUE) {
            throw new IllegalArgumentException("Computed offset is not a valid integer");
        }
        return (int) offset;
    }

    private static int hash(MiruPartitionCoord coord) {
        return new HashCodeBuilder().append(coord.tenantId).append(coord.partitionId).toHashCode();
    }

    @Override
    public LABEnvironment[] allocateLABEnvironments(MiruPartitionCoord coord) throws Exception {

        File[] labDirs = getLabDirs(coord);
        for (int i = 0; i < labDirs.length; i++) {
            FileUtils.deleteDirectory(labDirs[i]);
        }

        LABEnvironment[] environments = new LABEnvironment[labDirs.length];
        for (int i = 0; i < labDirs.length; i++) {
            environments[i] = new LABEnvironment(buildLABCompactorThreadPool,
                buildLABDestroyThreadPool,
                labDirs[i],
                true, 4, 16, 1024);

        }
        return environments;
    }

    @Override
    public boolean checkExists(MiruPartitionCoord coord) throws Exception {
        return true;
    }

    @Override
    public boolean hasChunkStores(MiruPartitionCoord coord) throws Exception {
        return !useLabIndexes;
    }

    @Override
    public boolean hasLabIndex(MiruPartitionCoord coord) throws Exception {
        return useLabIndexes;
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
