package com.jivesoftware.os.miru.service.stream;

import com.google.common.collect.Maps;
import com.jivesoftware.os.jive.utils.ordered.id.OrderIdProvider;
import com.jivesoftware.os.lab.LABEnvironment;
import com.jivesoftware.os.lab.LABRawhide;
import com.jivesoftware.os.lab.api.FormatTransformerProvider;
import com.jivesoftware.os.lab.api.RawEntryFormat;
import com.jivesoftware.os.lab.api.ValueIndex;
import com.jivesoftware.os.miru.plugin.cache.LabCacheKeyValues;
import com.jivesoftware.os.miru.plugin.cache.LabLastIdCacheKeyValues;
import com.jivesoftware.os.miru.plugin.cache.LabTimestampedCacheKeyValues;
import com.jivesoftware.os.miru.plugin.cache.MiruPluginCacheProvider;
import com.jivesoftware.os.miru.plugin.context.KeyValueRawhide;
import com.jivesoftware.os.miru.plugin.context.LastIdKeyValueRawhide;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import java.util.Map;

/**
 *
 */
public class LabPluginCacheProvider implements MiruPluginCacheProvider {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    private final OrderIdProvider idProvider;
    private final LABEnvironment[] labEnvironments;

    private final Map<String, LabCacheKeyValues> pluginPersistentCache = Maps.newConcurrentMap();
    private final Map<String, LabLastIdCacheKeyValues> lastIdPluginPersistentCache = Maps.newConcurrentMap();
    private final Map<String, LabTimestampedCacheKeyValues> timestampedPluginPersistentCache = Maps.newConcurrentMap();
    private final Object[] stripedLocks;

    public LabPluginCacheProvider(OrderIdProvider idProvider,
        LABEnvironment[] labEnvironments) {
        this.idProvider = idProvider;
        this.labEnvironments = labEnvironments;
        this.stripedLocks = new Object[Short.MAX_VALUE]; //TODO hmmm
        for (int i = 0; i < stripedLocks.length; i++) {
            stripedLocks[i] = new Object();
        }
    }

    @Override
    public CacheKeyValues getKeyValues(String name, int payloadSize, boolean variablePayloadSize, long maxHeapPressureInBytes) {
        return pluginPersistentCache.computeIfAbsent(name, (key) -> {
            try {
                ValueIndex[] cacheIndexes = new ValueIndex[labEnvironments.length];
                for (int i = 0; i < cacheIndexes.length; i++) {
                    // currently not commitable, as the commit is done immediately at write time
                    cacheIndexes[i] = labEnvironments[i].open("pluginCache-" + key, // serialization compatible with lastId keyValues
                        4096,
                        maxHeapPressureInBytes,
                        10 * 1024 * 1024,
                        -1L,
                        -1L,
                        FormatTransformerProvider.NO_OP,
                        new KeyValueRawhide(),
                        RawEntryFormat.MEMORY);
                }
                return new LabCacheKeyValues(name, idProvider, cacheIndexes);
            } catch (Exception x) {
                throw new RuntimeException("Failed to initialize plugin cache", x);
            }
        });
    }

    @Override
    public LastIdCacheKeyValues getLastIdKeyValues(String name, int payloadSize, boolean variablePayloadSize, long maxHeapPressureInBytes) {
        return lastIdPluginPersistentCache.computeIfAbsent(name, (key) -> {
            try {
                ValueIndex[] cacheIndexes = new ValueIndex[labEnvironments.length];
                for (int i = 0; i < cacheIndexes.length; i++) {
                    // currently not commitable, as the commit is done immediately at write time
                    cacheIndexes[i] = labEnvironments[i].open("pluginCache-" + key, // serialization compatible with plain keyValues
                        4096,
                        maxHeapPressureInBytes,
                        10 * 1024 * 1024,
                        -1L,
                        -1L,
                        FormatTransformerProvider.NO_OP,
                        new LastIdKeyValueRawhide(),
                        RawEntryFormat.MEMORY);
                }
                return new LabLastIdCacheKeyValues(name, idProvider, cacheIndexes);
            } catch (Exception x) {
                throw new RuntimeException("Failed to initialize plugin cache", x);
            }
        });
    }

    @Override
    public TimestampedCacheKeyValues getTimestampedKeyValues(String name, int payloadSize, boolean variablePayloadSize, long maxHeapPressureInBytes) {
        return timestampedPluginPersistentCache.computeIfAbsent(name, (key) -> {
            try {
                ValueIndex[] cacheIndexes = new ValueIndex[labEnvironments.length];
                for (int i = 0; i < cacheIndexes.length; i++) {
                    // currently not commitable, as the commit is done immediately at write time
                    cacheIndexes[i] = labEnvironments[i].open("timestampedCache-" + key,
                        4096,
                        maxHeapPressureInBytes,
                        10 * 1024 * 1024,
                        -1L,
                        -1L,
                        FormatTransformerProvider.NO_OP,
                        new LABRawhide(),
                        RawEntryFormat.MEMORY);
                }
                return new LabTimestampedCacheKeyValues(name, idProvider, cacheIndexes, stripedLocks);
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
        for (LabTimestampedCacheKeyValues cache : timestampedPluginPersistentCache.values()) {
            try {
                cache.commit(fsyncOnCommit);
            } catch (Exception e) {
                LOG.error("Failed to close timestamped plugin cache {}", new Object[] { cache.name() }, e);
            }
        }
    }

    public void close(boolean flushUncommited, boolean fsync) throws Exception {
        for (LabCacheKeyValues cacheKeyValues : pluginPersistentCache.values()) {
            cacheKeyValues.close(flushUncommited, fsync);
        }
        for (LabTimestampedCacheKeyValues cacheKeyValues : timestampedPluginPersistentCache.values()) {
            cacheKeyValues.close(flushUncommited, fsync);
        }
    }
}
