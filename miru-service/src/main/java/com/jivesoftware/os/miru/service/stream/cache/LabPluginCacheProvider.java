package com.jivesoftware.os.miru.service.stream.cache;

import com.google.common.collect.Maps;
import com.jivesoftware.os.filer.io.ByteArrayStripingLocksProvider;
import com.jivesoftware.os.jive.utils.ordered.id.OrderIdProvider;
import com.jivesoftware.os.lab.LABEnvironment;
import com.jivesoftware.os.lab.api.MemoryRawEntryFormat;
import com.jivesoftware.os.lab.api.NoOpFormatTransformerProvider;
import com.jivesoftware.os.lab.api.ValueIndex;
import com.jivesoftware.os.lab.api.ValueIndexConfig;
import com.jivesoftware.os.lab.api.rawhide.KeyValueRawhide;
import com.jivesoftware.os.lab.api.rawhide.LABRawhide;
import com.jivesoftware.os.lab.guts.LABHashIndexType;
import com.jivesoftware.os.lab.io.api.UIO;
import com.jivesoftware.os.miru.plugin.bitmap.MiruBitmaps;
import com.jivesoftware.os.miru.plugin.cache.LabCacheKeyValues;
import com.jivesoftware.os.miru.plugin.cache.LabLastIdCacheKeyValues;
import com.jivesoftware.os.miru.plugin.cache.LabTimestampedCacheKeyValues;
import com.jivesoftware.os.miru.plugin.cache.MiruPluginCacheProvider;
import com.jivesoftware.os.miru.plugin.partition.TrackError;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import java.util.Map;

/**
 *
 */
public class LabPluginCacheProvider<BM extends IBM, IBM> implements MiruPluginCacheProvider<BM, IBM> {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    private final OrderIdProvider idProvider;
    private final LABEnvironment[] labEnvironments;
    private final LabPluginCacheProviderLock[] stripedLocks;
    private final ByteArrayStripingLocksProvider byteArrayStripingLocksProvider;
    private final MiruBitmaps<BM, IBM> bitmaps;
    private final TrackError trackError;
    private final boolean atomized;
    private final boolean hashIndexEnabled;

    private final Map<String, LabCacheKeyValues> pluginPersistentCache = Maps.newConcurrentMap();
    private final Map<String, LabLastIdCacheKeyValues> lastIdPluginPersistentCache = Maps.newConcurrentMap();
    private final Map<String, LabCacheKeyBitmaps<BM, IBM>> bitmapPluginPersistentCache = Maps.newConcurrentMap();
    private final Map<String, LabTimestampedCacheKeyValues> timestampedPluginPersistentCache = Maps.newConcurrentMap();

    public LabPluginCacheProvider(OrderIdProvider idProvider,
        LABEnvironment[] labEnvironments,
        LabPluginCacheProviderLock[] stripedLocks,
        ByteArrayStripingLocksProvider byteArrayStripingLocksProvider,
        MiruBitmaps<BM, IBM> bitmaps,
        TrackError trackError,
        boolean atomized,
        boolean hashIndexEnabled) {
        this.idProvider = idProvider;
        this.labEnvironments = labEnvironments;
        this.stripedLocks = stripedLocks;
        this.byteArrayStripingLocksProvider = byteArrayStripingLocksProvider;
        this.bitmaps = bitmaps;
        this.trackError = trackError;
        this.atomized = atomized;
        this.hashIndexEnabled = hashIndexEnabled;
    }

    @Override
    public CacheKeyValues getKeyValues(String name,
        int payloadSize,
        boolean variablePayloadSize,
        long maxHeapPressureInBytes,
        String hashIndexType,
        double hashIndexLoadFactor) {

        return pluginPersistentCache.computeIfAbsent(name, (key) -> {
            try {
                ValueIndex<byte[]>[] cacheIndexes = new ValueIndex[labEnvironments.length];
                for (int i = 0; i < cacheIndexes.length; i++) {
                    // currently not commitable, as the commit is done immediately at write time
                    cacheIndexes[i] = labEnvironments[i].open(new ValueIndexConfig("pluginCache-" + key, // serialization compatible with lastId keyValues
                        4096,
                        maxHeapPressureInBytes,
                        -1L,
                        -1L,
                        100 * 1024 * 1024,
                        NoOpFormatTransformerProvider.NAME,
                        KeyValueRawhide.NAME,
                        MemoryRawEntryFormat.NAME,
                        UIO.chunkPower(payloadSize * 1024, 4),
                        LABHashIndexType.valueOf(hashIndexType),
                        hashIndexLoadFactor,
                        hashIndexEnabled));
                }
                return new LabCacheKeyValues(name, idProvider, cacheIndexes);
            } catch (Exception x) {
                throw new RuntimeException("Failed to initialize plugin cache", x);
            }
        });
    }

    @Override
    public LastIdCacheKeyValues getLastIdKeyValues(String name,
        int payloadSize,
        boolean variablePayloadSize,
        long maxHeapPressureInBytes,
        String hashIndexType,
        double hashIndexLoadFactor) {

        return lastIdPluginPersistentCache.computeIfAbsent(name, (key) -> {
            try {
                ValueIndex<byte[]>[] cacheIndexes = new ValueIndex[labEnvironments.length];
                for (int i = 0; i < cacheIndexes.length; i++) {
                    // currently not commitable, as the commit is done immediately at write time
                    cacheIndexes[i] = labEnvironments[i].open(new ValueIndexConfig("pluginCache-" + key, // serialization compatible with plain keyValues
                        4096,
                        maxHeapPressureInBytes,
                        -1L,
                        -1L,
                        100 * 1024 * 1024,
                        NoOpFormatTransformerProvider.NAME,
                        "lastIdKeyValue",
                        MemoryRawEntryFormat.NAME,
                        UIO.chunkPower(payloadSize * 1024, 4),
                        LABHashIndexType.valueOf(hashIndexType),
                        hashIndexLoadFactor,
                        hashIndexEnabled));
                }
                return new LabLastIdCacheKeyValues(name, idProvider, cacheIndexes);
            } catch (Exception x) {
                throw new RuntimeException("Failed to initialize plugin cache", x);
            }
        });
    }

    @Override
    public TimestampedCacheKeyValues getTimestampedKeyValues(String name,
        int payloadSize,
        boolean variablePayloadSize,
        long maxHeapPressureInBytes,
        String hashIndexType,
        double hashIndexLoadFactor) {
        return timestampedPluginPersistentCache.computeIfAbsent(name, (key) -> {
            try {
                ValueIndex<byte[]>[] cacheIndexes = new ValueIndex[labEnvironments.length];
                for (int i = 0; i < cacheIndexes.length; i++) {
                    // currently not commitable, as the commit is done immediately at write time
                    cacheIndexes[i] = labEnvironments[i].open(new ValueIndexConfig("timestampedCache-" + key,
                        4096,
                        maxHeapPressureInBytes,
                        -1L,
                        -1L,
                        100 * 1024 * 1024,
                        NoOpFormatTransformerProvider.NAME,
                        LABRawhide.NAME,
                        MemoryRawEntryFormat.NAME,
                        UIO.chunkPower(payloadSize * 1024, 4),
                        LABHashIndexType.valueOf(hashIndexType),
                        hashIndexLoadFactor,
                        hashIndexEnabled));
                }
                return new LabTimestampedCacheKeyValues(name, idProvider, cacheIndexes, stripedLocks);
            } catch (Exception x) {
                throw new RuntimeException("Failed to initialize plugin cache", x);
            }
        });
    }

    @Override
    public CacheKeyBitmaps<BM, IBM> getCacheKeyBitmaps(String name,
        int payloadSize,
        long maxHeapPressureInBytes,
        String hashIndexType,
        double hashIndexLoadFactor) {
        return bitmapPluginPersistentCache.computeIfAbsent(name, (key) -> {
            try {
                ValueIndex<byte[]>[] cacheIndexes = new ValueIndex[labEnvironments.length];
                for (int i = 0; i < cacheIndexes.length; i++) {
                    // currently not commitable, as the commit is done immediately at write time
                    cacheIndexes[i] = labEnvironments[i].open(new ValueIndexConfig("bitmapCache-" + key,
                        4096,
                        maxHeapPressureInBytes,
                        -1L,
                        -1L,
                        100 * 1024 * 1024,
                        NoOpFormatTransformerProvider.NAME,
                        LABRawhide.NAME,
                        MemoryRawEntryFormat.NAME,
                        UIO.chunkPower(payloadSize * 1024, 4),
                        LABHashIndexType.valueOf(hashIndexType),
                        hashIndexLoadFactor,
                        hashIndexEnabled));
                }
                return new LabCacheKeyBitmaps<>(name, idProvider, bitmaps, trackError, atomized, cacheIndexes, byteArrayStripingLocksProvider);
            } catch (Exception x) {
                throw new RuntimeException("Failed to initialize plugin cache", x);
            }
        });
    }

    public void commit(boolean fsyncOnCommit) throws Exception {
        for (LabCacheKeyValues cache : pluginPersistentCache.values()) {
            try {
                cache.commit(fsyncOnCommit);
            } catch (Exception e) {
                LOG.error("Failed to close plugin cache {}", new Object[] { cache.name() }, e);
            }
        }
        for (LabLastIdCacheKeyValues cache : lastIdPluginPersistentCache.values()) {
            try {
                cache.commit(fsyncOnCommit);
            } catch (Exception e) {
                LOG.error("Failed to close lastId plugin cache {}", new Object[] { cache.name() }, e);
            }
        }
        for (LabTimestampedCacheKeyValues cache : timestampedPluginPersistentCache.values()) {
            try {
                cache.commit(fsyncOnCommit);
            } catch (Exception e) {
                LOG.error("Failed to close timestamped plugin cache {}", new Object[] { cache.name() }, e);
            }
        }
        for (LabCacheKeyBitmaps<BM, IBM> cache : bitmapPluginPersistentCache.values()) {
            try {
                cache.commit(fsyncOnCommit);
            } catch (Exception e) {
                LOG.error("Failed to close bitmaps plugin cache {}", new Object[] { cache.name() }, e);
            }
        }
    }

    public void close(boolean flushUncommited, boolean fsync) throws Exception {
        for (LabCacheKeyValues cache : pluginPersistentCache.values()) {
            cache.close(flushUncommited, fsync);
        }
        for (LabLastIdCacheKeyValues cache : lastIdPluginPersistentCache.values()) {
            cache.close(flushUncommited, fsync);
        }
        for (LabTimestampedCacheKeyValues cache : timestampedPluginPersistentCache.values()) {
            cache.close(flushUncommited, fsync);
        }
        for (LabCacheKeyBitmaps<BM, IBM> cache : bitmapPluginPersistentCache.values()) {
            cache.close(flushUncommited, fsync);
        }
    }

    public void compact(boolean fsync, int minDebt, int maxDebt, boolean waitIfTooFarBehind) throws Exception {
        for (LabCacheKeyValues cache : pluginPersistentCache.values()) {
            cache.compact(fsync, minDebt, maxDebt, waitIfTooFarBehind);
        }
        for (LabLastIdCacheKeyValues cache : lastIdPluginPersistentCache.values()) {
            cache.compact(fsync, minDebt, maxDebt, waitIfTooFarBehind);
        }
        for (LabTimestampedCacheKeyValues cache : timestampedPluginPersistentCache.values()) {
            cache.compact(fsync, minDebt, maxDebt, waitIfTooFarBehind);
        }
        for (LabCacheKeyBitmaps<BM, IBM> cache : bitmapPluginPersistentCache.values()) {
            cache.compact(fsync, minDebt, maxDebt, waitIfTooFarBehind);
        }
    }

    public static LabPluginCacheProviderLock[] allocateLocks(int count) {
        LabPluginCacheProviderLock[] locks = new LabPluginCacheProviderLock[count];
        for (int i = 0; i < locks.length; i++) {
            locks[i] = new LabPluginCacheProviderLock();
        }
        return locks;
    }

    public static class LabPluginCacheProviderLock {
    }
}
