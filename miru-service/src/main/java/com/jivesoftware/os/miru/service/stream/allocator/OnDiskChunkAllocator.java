package com.jivesoftware.os.miru.service.stream.allocator;

import com.google.common.collect.Lists;
import com.jivesoftware.os.filer.chunk.store.ChunkStoreInitializer;
import com.jivesoftware.os.filer.io.ByteBufferFactory;
import com.jivesoftware.os.filer.io.api.StackBuffer;
import com.jivesoftware.os.filer.io.chunk.ChunkStore;
import com.jivesoftware.os.lab.LABEnvironment;
import com.jivesoftware.os.miru.api.MiruPartitionCoord;
import com.jivesoftware.os.miru.service.locator.MiruPartitionCoordIdentifier;
import com.jivesoftware.os.miru.service.locator.MiruResourceLocator;
import com.jivesoftware.os.miru.service.locator.MiruResourcePartitionIdentifier;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import java.io.File;
import java.util.List;
import java.util.concurrent.ExecutorService;
import org.apache.commons.lang.builder.HashCodeBuilder;

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
    private final ChunkStoreInitializer chunkStoreInitializer = new ChunkStoreInitializer();
    private final ExecutorService buildLABCompactorThreadPool = LABEnvironment.buildLABCompactorThreadPool(12);
    private final ExecutorService buildLABDestroyThreadPool = LABEnvironment.buildLABDestroyThreadPool(12);

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

    public static void main(String[] args) {

        int count = 3;
        for (int hashCode : new int[] { -13, -7, -2, -1, 0, 1, 2, 7, 13 }) {
            for (int shift = 0; shift < count; shift++) {
                System.out.println("--- " + hashCode + ", " + shift);
                for (int i = 0; i < count; i++) {
                    System.out.println("" + offset(hashCode, i, shift, count));
                }
            }
        }
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
    public boolean checkExists(MiruPartitionCoord coord) throws Exception {
        return hasChunkStores(coord) || hasLabIndex(coord);
    }

    @Override
    public boolean hasChunkStores(MiruPartitionCoord coord) throws Exception {
        MiruPartitionCoordIdentifier identifier = new MiruPartitionCoordIdentifier(coord);
        File[] chunkDirs = resourceLocator.getChunkDirectories(identifier, "chunks");
        int hashCode = hash(coord);
        // since the hashCode function can change, we need to be overly defensive and check all possible directories for each chunk
        for (int i = 0; i < numberOfChunkStores; i++) {
            boolean found = false;
            for (int shift = 0; shift < chunkDirs.length; shift++) {
                int checkOffset = offset(hashCode, i, shift, chunkDirs.length);
                if (chunkStoreInitializer.checkExists(chunkDirs, checkOffset, "chunk-" + i)) {
                    found = true;
                    break;
                }
            }
            if (!found) {
                log.warn("Partition missing chunk {} for {}", i, coord);
                return false;
            }
        }

        log.info("Partition has chunks on disk for {}", coord);
        return true;
    }

    @Override
    public boolean hasLabIndex(MiruPartitionCoord coord) throws Exception {
        MiruPartitionCoordIdentifier identifier = new MiruPartitionCoordIdentifier(coord);
        File[] baseDirs = resourceLocator.getChunkDirectories(identifier, "labs");
        int hashCode = hash(coord);
        for (int i = 0; i < numberOfChunkStores; i++) {
            int directoryOffset = offset(hashCode, i, 0, baseDirs.length);
            File dir = new File(baseDirs[directoryOffset], "lab-" + i);
            if (!dir.exists()) {
                log.warn("Partition missing lab {} for {}", i, coord);
                return false;
            }
        }

        log.info("Partition has labs on disk for {}", coord);
        return true;
    }

    @Override
    public ChunkStore[] allocateChunkStores(MiruPartitionCoord coord) throws Exception {

        MiruResourcePartitionIdentifier identifier = new MiruPartitionCoordIdentifier(coord);

        File[] chunkDirs = resourceLocator.getChunkDirectories(identifier, "chunks");
        ChunkStore[] chunkStores = new ChunkStore[numberOfChunkStores];
        int hashCode = hash(coord);
        // since the hashCode function can change, we need to be overly defensive and check all possible directories for each chunk
        StackBuffer stackBuffer = new StackBuffer();
        for (int i = 0; i < numberOfChunkStores; i++) {
            int directoryOffset = offset(hashCode, i, 0, chunkDirs.length);
            for (int shift = 0; shift < chunkDirs.length; shift++) {
                int checkOffset = offset(hashCode, i, shift, chunkDirs.length);
                if (chunkStoreInitializer.checkExists(chunkDirs, checkOffset, "chunk-" + i)) {
                    directoryOffset = checkOffset;
                    break;
                }
            }
            chunkStores[i] = chunkStoreInitializer.openOrCreate(
                chunkDirs,
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
    public File[] getLabDirs(MiruPartitionCoord coord) throws Exception {
        MiruResourcePartitionIdentifier identifier = new MiruPartitionCoordIdentifier(coord);
        File[] baseDirs = resourceLocator.getChunkDirectories(identifier, "labs");
        int hashCode = hash(coord);
        List<File> dirs = Lists.newArrayList();
        for (int i = 0; i < numberOfChunkStores; i++) {
            int directoryOffset = offset(hashCode, i, 0, baseDirs.length);
            dirs.add(new File(baseDirs[directoryOffset], "lab-" + i));
        }
        return dirs.toArray(new File[0]);
    }

    @Override
    public LABEnvironment[] allocateLABEnvironments(MiruPartitionCoord coord) throws Exception {
        File[] labDirs = getLabDirs(coord);
        LABEnvironment[] environments = new LABEnvironment[labDirs.length];
        for (int i = 0; i < numberOfChunkStores; i++) {
            environments[i] = new LABEnvironment(buildLABCompactorThreadPool, buildLABDestroyThreadPool, labDirs[i],
                true, 4, 16, 1024);
        }
        return environments;
    }

    @Override
    public void close(ChunkStore[] chunkStores) {
    }

}
