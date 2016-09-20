package com.jivesoftware.os.miru.service.stream.allocator;

import com.google.common.collect.Lists;
import com.jivesoftware.os.filer.chunk.store.ChunkStoreInitializer;
import com.jivesoftware.os.filer.io.ByteBufferFactory;
import com.jivesoftware.os.filer.io.api.StackBuffer;
import com.jivesoftware.os.filer.io.chunk.ChunkStore;
import com.jivesoftware.os.jive.utils.collections.bah.LRUConcurrentBAHLinkedHash;
import com.jivesoftware.os.lab.LABEnvironment;
import com.jivesoftware.os.lab.LABStats;
import com.jivesoftware.os.lab.LabHeapPressure;
import com.jivesoftware.os.lab.api.rawhide.FixedWidthRawhide;
import com.jivesoftware.os.lab.guts.Leaps;
import com.jivesoftware.os.lab.guts.StripingBolBufferLocks;
import com.jivesoftware.os.miru.api.MiruPartitionCoord;
import com.jivesoftware.os.miru.plugin.context.LastIdKeyValueRawhide;
import com.jivesoftware.os.miru.service.locator.MiruPartitionCoordIdentifier;
import com.jivesoftware.os.miru.service.locator.MiruResourceLocator;
import com.jivesoftware.os.miru.service.locator.MiruResourcePartitionIdentifier;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.ExecutorService;
import org.apache.commons.lang.builder.HashCodeBuilder;

/**
 *
 */
public class OnDiskChunkAllocator implements MiruChunkAllocator {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    private final MiruResourceLocator resourceLocator;
    private final ByteBufferFactory cacheByteBufferFactory;
    private final int numberOfChunkStores;
    private final int partitionInitialChunkCacheSize;
    private final int partitionMaxChunkCacheSize;
    private final LABStats[] labStats;
    private final LabHeapPressure[] labHeapPressures;
    private final long labMaxWALSizeInBytes;
    private final long labMaxEntriesPerWAL;
    private final long labMaxEntrySizeInBytes;
    private final long labMaxWALOnOpenHeapPressureOverride;
    private final boolean labUseOffHeap;
    private final LRUConcurrentBAHLinkedHash<Leaps> leapCache;
    private final  StripingBolBufferLocks bolBufferLocks;
    private final ChunkStoreInitializer chunkStoreInitializer = new ChunkStoreInitializer();

    private final ExecutorService buildLABSchedulerThreadPool = LABEnvironment.buildLABSchedulerThreadPool(12);
    private final ExecutorService buildLABCompactorThreadPool = LABEnvironment.buildLABCompactorThreadPool(12); // TODO config
    private final ExecutorService buildLABDestroyThreadPool = LABEnvironment.buildLABDestroyThreadPool(12); // TODO config

    public OnDiskChunkAllocator(
        MiruResourceLocator resourceLocator,
        ByteBufferFactory cacheByteBufferFactory,
        int numberOfChunkStores,
        int partitionInitialChunkCacheSize,
        int partitionMaxChunkCacheSize,
        LABStats[] labStats,
        LabHeapPressure[] labHeapPressures,
        long labMaxWALSizeInBytes,
        long labMaxEntriesPerWAL,
        long labMaxEntrySizeInBytes,
        long labMaxWALOnOpenHeapPressureOverride,
        boolean labUseOffHeap,
        LRUConcurrentBAHLinkedHash<Leaps> leapCache,
        StripingBolBufferLocks bolBufferLocks) {
        this.resourceLocator = resourceLocator;
        this.cacheByteBufferFactory = cacheByteBufferFactory;
        this.numberOfChunkStores = numberOfChunkStores;
        this.partitionInitialChunkCacheSize = partitionInitialChunkCacheSize;
        this.partitionMaxChunkCacheSize = partitionMaxChunkCacheSize;
        this.labStats = labStats;
        this.labHeapPressures = labHeapPressures;
        this.labMaxWALSizeInBytes = labMaxWALSizeInBytes;
        this.labMaxEntriesPerWAL = labMaxEntriesPerWAL;
        this.labMaxEntrySizeInBytes = labMaxEntrySizeInBytes;
        this.labMaxWALOnOpenHeapPressureOverride = labMaxWALOnOpenHeapPressureOverride;
        this.labUseOffHeap = labUseOffHeap;
        this.leapCache = leapCache;
        this.bolBufferLocks = bolBufferLocks;
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
    public boolean checkExists(MiruPartitionCoord coord, int labVersion, int[] supportedLabVersions) throws Exception {
        if (hasLabIndex(coord, labVersion)) {
            return true;
        }
        for (int supportedLabVersion : supportedLabVersions) {
            if (hasLabIndex(coord, supportedLabVersion)) {
                return true;
            }
        }
        return hasChunkStores(coord);
    }

    @Override
    public boolean hasChunkStores(MiruPartitionCoord coord) throws Exception {
        MiruPartitionCoordIdentifier identifier = new MiruPartitionCoordIdentifier(coord);
        File[] chunkDirs = resourceLocator.getChunkDirectories(identifier, "chunks", -1);
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
                LOG.warn("Partition missing chunk {} for {}", i, coord);
                return false;
            }
        }

        LOG.info("Partition has chunks on disk for {}", coord);
        return true;
    }

    @Override
    public boolean hasLabIndex(MiruPartitionCoord coord, int version) throws Exception {
        MiruPartitionCoordIdentifier identifier = new MiruPartitionCoordIdentifier(coord);
        File[] baseDirs = resourceLocator.getChunkDirectories(identifier, "labs", version);
        int hashCode = hash(coord);
        for (int i = 0; i < numberOfChunkStores; i++) {
            int directoryOffset = offset(hashCode, i, 0, baseDirs.length);
            File dir = new File(baseDirs[directoryOffset], "lab-" + i);
            if (!dir.exists()) {
                LOG.warn("Partition missing lab {} for {} version {}", i, coord, version);
                return false;
            }
        }

        LOG.info("Partition has labs on disk for {} version {}", coord, version);
        return true;
    }

    @Override
    public ChunkStore[] allocateChunkStores(MiruPartitionCoord coord) throws Exception {

        MiruResourcePartitionIdentifier identifier = new MiruPartitionCoordIdentifier(coord);

        File[] chunkDirs = resourceLocator.getChunkDirectories(identifier, "chunks", -1);
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
    public File[] getLabDirs(MiruPartitionCoord coord, int version) throws Exception {
        MiruResourcePartitionIdentifier identifier = new MiruPartitionCoordIdentifier(coord);
        File[] baseDirs = resourceLocator.getChunkDirectories(identifier, "labs", version);
        int hashCode = hash(coord);
        List<File> dirs = Lists.newArrayList();
        for (int i = 0; i < numberOfChunkStores; i++) {
            int directoryOffset = offset(hashCode, i, 0, baseDirs.length);
            dirs.add(new File(baseDirs[directoryOffset], "lab-" + i));
        }
        return dirs.toArray(new File[0]);
    }

    @Override
    public LABEnvironment[] allocateLABEnvironments(MiruPartitionCoord coord, int version) throws Exception {
        return allocateLABEnvironments(getLabDirs(coord, version));
    }

    @Override
    public LABEnvironment[] allocateLABEnvironments(File[] labDirs) throws Exception {
        LABEnvironment[] environments = new LABEnvironment[labDirs.length];
        for (int i = 0; i < labDirs.length; i++) {
            labDirs[i].mkdirs();
            environments[i] = new LABEnvironment(labStats[i % labStats.length],
                buildLABSchedulerThreadPool,
                buildLABCompactorThreadPool,
                buildLABDestroyThreadPool,
                "wal",
                labMaxWALSizeInBytes,
                labMaxEntriesPerWAL,
                labMaxEntrySizeInBytes,
                labMaxWALOnOpenHeapPressureOverride,
                labDirs[i],
                labHeapPressures[i % labHeapPressures.length],
                4,
                16,
                leapCache,
                bolBufferLocks,
                labUseOffHeap);

            environments[i].register("lastIdKeyValue", new LastIdKeyValueRawhide());
            environments[i].register("fixedWidth_12_0", new FixedWidthRawhide(12, 0));
            environments[i].register("fixedWidth_8_4", new FixedWidthRawhide(8, 4));
            environments[i].register("fixedWidth_4_16", new FixedWidthRawhide(4, 16));
            environments[i].register("fixedWidth_4_17", new FixedWidthRawhide(4, 17));
            environments[i].open();
        }
        return environments;
    }

    @Override
    public void close(ChunkStore[] chunkStores) {
    }

    @Override
    public void remove(ChunkStore[] chunkStores) {
        for (ChunkStore chunkStore : chunkStores) {
            try {
                chunkStore.delete();
            } catch (IOException e) {
                LOG.warn("Failed to delete chunk store", e);
            }
        }
    }

    @Override
    public void close(LABEnvironment[] labEnvironments) throws Exception {
        for (LABEnvironment labEnvironment : labEnvironments) {
            labEnvironment.close();
        }
    }

    @Override
    public void remove(LABEnvironment[] labEnvironments) {
        for (LABEnvironment labEnvironment : labEnvironments) {
            try {
                labEnvironment.delete();
            } catch (IOException e) {
                LOG.warn("Failed to delete lab", e);
            }
        }
    }
}
