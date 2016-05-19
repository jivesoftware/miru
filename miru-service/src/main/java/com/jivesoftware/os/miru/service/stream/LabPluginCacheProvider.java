package com.jivesoftware.os.miru.service.stream;

import com.google.common.collect.Maps;
import com.jivesoftware.os.jive.utils.ordered.id.OrderIdProvider;
import com.jivesoftware.os.lab.LABEnvironment;
import com.jivesoftware.os.lab.LABRawhide;
import com.jivesoftware.os.lab.api.ValueIndex;
import com.jivesoftware.os.miru.plugin.cache.LabCacheKeyValues;
import com.jivesoftware.os.miru.plugin.cache.LabTimestampedCacheKeyValues;
import com.jivesoftware.os.miru.plugin.cache.MiruPluginCacheProvider;
import com.jivesoftware.os.miru.plugin.context.KeyValueRawhide;
import java.util.Map;

/**
 *
 */
public class LabPluginCacheProvider implements MiruPluginCacheProvider {

    private final OrderIdProvider idProvider;
    private final LABEnvironment[] labEnvironments;

    private final Map<String, CacheKeyValues> pluginPersistentCache = Maps.newConcurrentMap();
    private final Map<String, TimestampedCacheKeyValues> timestampedPluginPersistentCache = Maps.newConcurrentMap();
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
    public CacheKeyValues getKeyValues(String name, int payloadSize, boolean variablePayloadSize, int maxUpdatesBeforeFlush) {
        return pluginPersistentCache.computeIfAbsent(name, (key) -> {
            try {
                ValueIndex[] cacheIndexes = new ValueIndex[labEnvironments.length];
                for (int i = 0; i < cacheIndexes.length; i++) {
                    // currently not commitable, as the commit is done immediately at write time
                    cacheIndexes[i] = labEnvironments[i].open("pluginCache-" + key,
                        4096,
                        maxUpdatesBeforeFlush,
                        10 * 1024 * 1024,
                        -1L,
                        -1L,
                        new KeyValueRawhide());
                }
                return new LabCacheKeyValues(idProvider, cacheIndexes);
            } catch (Exception x) {
                throw new RuntimeException("Failed to initialize plugin cache", x);
            }
        });
    }

    @Override
    public TimestampedCacheKeyValues getTimestampedKeyValues(String name, int payloadSize, boolean variablePayloadSize, int maxUpdatesBeforeFlush) {
        return timestampedPluginPersistentCache.computeIfAbsent(name, (key) -> {
            try {
                ValueIndex[] cacheIndexes = new ValueIndex[labEnvironments.length];
                for (int i = 0; i < cacheIndexes.length; i++) {
                    // currently not commitable, as the commit is done immediately at write time
                    cacheIndexes[i] = labEnvironments[i].open("timestampedCache-" + key,
                        4096,
                        maxUpdatesBeforeFlush,
                        10 * 1024 * 1024,
                        -1L,
                        -1L,
                        new LABRawhide());
                }
                return new LabTimestampedCacheKeyValues(idProvider, cacheIndexes, stripedLocks);
            } catch (Exception x) {
                throw new RuntimeException("Failed to initialize plugin cache", x);
            }
        });
    }
}
