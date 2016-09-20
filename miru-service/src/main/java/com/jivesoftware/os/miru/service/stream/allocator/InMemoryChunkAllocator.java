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
    private final LABStats[] labStats;
    private final LabHeapPressure[] labHeapPressures;
    private final long labMaxWALSizeInBytes;
    private final long labMaxEntriesPerWAL;
    private final long labMaxEntrySizeInBytes;
    private final long labMaxWALOnOpenHeapPressureOverride;
    private final boolean labUseOffHeap;
    private final LRUConcurrentBAHLinkedHash<Leaps> leapCache;
    private final StripingBolBufferLocks bolBufferLocks;
    private final boolean useLabIndexes;

    private final ExecutorService buildLABSchedulerThreadPool = LABEnvironment.buildLABSchedulerThreadPool(12);
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
        LABStats[] labStats,
        LabHeapPressure[] labHeapPressures,
        long labMaxWALSizeInBytes,
        long labMaxEntriesPerWAL,
        long labMaxEntrySizeInBytes,
        long labMaxWALOnOpenHeapPressureOverride,
        boolean labUseOffHeap,
        boolean useLabIndexes,
        LRUConcurrentBAHLinkedHash<Leaps> leapCache,
        StripingBolBufferLocks bolBufferLocks) {
        this.resourceLocator = resourceLocator;
        this.rebuildByteBufferFactory = rebuildByteBufferFactory;
        this.cacheByteBufferFactory = cacheByteBufferFactory;
        this.initialChunkSize = initialChunkSize;
        this.numberOfChunkStores = numberOfChunkStores;
        this.partitionDeleteChunkStoreOnClose = partitionDeleteChunkStoreOnClose;
        this.partitionInitialChunkCacheSize = partitionInitialChunkCacheSize;
        this.partitionMaxChunkCacheSize = partitionMaxChunkCacheSize;
        this.labMaxWALSizeInBytes = labMaxWALSizeInBytes;
        this.labMaxEntriesPerWAL = labMaxEntriesPerWAL;
        this.labMaxEntrySizeInBytes = labMaxEntrySizeInBytes;
        this.labMaxWALOnOpenHeapPressureOverride = labMaxWALOnOpenHeapPressureOverride;
        this.labUseOffHeap = labUseOffHeap;
        this.useLabIndexes = useLabIndexes;
        this.labStats = labStats;
        this.labHeapPressures = labHeapPressures;
        this.leapCache = leapCache;
        this.bolBufferLocks = bolBufferLocks;
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
    public File[] getLabDirs(MiruPartitionCoord coord, int version) throws Exception {
        MiruResourcePartitionIdentifier identifier = new TransientPartitionCoordIdentifier(coord);
        File[] baseDirs = resourceLocator.getChunkDirectories(identifier, "labs-transient", version);
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
    public LABEnvironment[] allocateLABEnvironments(MiruPartitionCoord coord, int version) throws Exception {
        File[] labDirs = getLabDirs(coord, version);
        for (int i = 0; i < labDirs.length; i++) {
            FileUtils.deleteDirectory(labDirs[i]);
        }
        return allocateLABEnvironments(labDirs);
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
    public boolean checkExists(MiruPartitionCoord coord, int labVersion, int[] supportedLabVersions) throws Exception {
        return true;
    }

    @Override
    public boolean hasChunkStores(MiruPartitionCoord coord) throws Exception {
        return !useLabIndexes;
    }

    @Override
    public boolean hasLabIndex(MiruPartitionCoord coord, int version) throws Exception {
        return useLabIndexes;
    }

    @Override
    public void close(ChunkStore[] chunkStores) {
    }

    @Override
    public void remove(ChunkStore[] chunkStores) {
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

    @Override
    public void close(LABEnvironment[] labEnvironments) throws Exception {
        for (LABEnvironment labEnvironment : labEnvironments) {
            labEnvironment.close();
        }
    }

    @Override
    public void remove(LABEnvironment[] labEnvironments) {
        if (partitionDeleteChunkStoreOnClose) {
            for (LABEnvironment labEnvironment : labEnvironments) {
                try {
                    labEnvironment.delete();
                } catch (IOException e) {
                    LOG.warn("Failed to delete lab", e);
                }
            }
        }
    }
}
