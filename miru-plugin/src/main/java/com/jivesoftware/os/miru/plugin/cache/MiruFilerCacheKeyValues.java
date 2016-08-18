package com.jivesoftware.os.miru.plugin.cache;

import com.google.common.base.Throwables;
import com.jivesoftware.os.filer.io.FilerIO;
import com.jivesoftware.os.filer.io.api.KeyedFilerStore;
import com.jivesoftware.os.filer.io.api.StackBuffer;
import com.jivesoftware.os.filer.io.chunk.ChunkFiler;
import com.jivesoftware.os.filer.io.map.MapContext;
import com.jivesoftware.os.filer.io.map.MapStore;
import com.jivesoftware.os.miru.plugin.cache.MiruPluginCacheProvider.CacheKeyValues;
import com.jivesoftware.os.miru.plugin.cache.MiruPluginCacheProvider.IndexKeyValueStream;
import com.jivesoftware.os.miru.plugin.cache.MiruPluginCacheProvider.KeyValueStream;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import java.io.IOException;
import java.nio.ByteBuffer;

/**
 *
 */
public class MiruFilerCacheKeyValues implements CacheKeyValues {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    private final String name;
    private final KeyedFilerStore<Integer, MapContext>[] powerIndex;

    public MiruFilerCacheKeyValues(String name, KeyedFilerStore<Integer, MapContext>[] powerIndex) {
        this.name = name;
        this.powerIndex = powerIndex;
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public boolean get(byte[] cacheId, byte[][] keys, IndexKeyValueStream stream, StackBuffer stackBuffer) throws Exception {
        byte[][][] powerKeyBytes = new byte[powerIndex.length][][];
        for (int i = 0; i < keys.length; i++) {
            if (keys[i] != null) {
                byte[] keyBytes = keys[i];
                int power = FilerIO.chunkPower(keyBytes.length, 0);
                if (powerKeyBytes[power] == null) {
                    powerKeyBytes[power] = new byte[keys.length][];
                }
                powerKeyBytes[power][i] = keyBytes;
            }
        }

        for (int power = 0; power < powerKeyBytes.length; power++) {
            long expectedKeySize = FilerIO.chunkLength(power);

            byte[][] keyBytes = powerKeyBytes[power];
            if (keyBytes != null) {
                powerIndex[power].read(cacheId, null, (monkey, filer, _stackBuffer, lock) -> {
                    try {
                        if (filer != null) {
                            if (monkey.keySize != expectedKeySize) {
                                throw new IllegalStateException("provided " + monkey.keySize + " expected " + expectedKeySize);
                            }
                            synchronized (lock) {
                                for (int i = 0; i < keyBytes.length; i++) {
                                    if (keyBytes[i] != null) {
                                        byte[] payload = MapStore.INSTANCE.getPayload(filer, monkey, keyBytes[i], _stackBuffer);
                                        if (!stream.stream(i, ByteBuffer.wrap(payload))) {
                                            return null;
                                        }
                                    }
                                }
                            }
                        } else {
                            for (int i = 0; i < keyBytes.length; i++) {
                                if (keyBytes[i] != null) {
                                    if (!stream.stream(i, null)) {
                                        return null;
                                    }
                                }
                            }
                        }
                        return null;
                    } catch (IOException e) {
                        throw e;
                    } catch (Exception e) {
                        throw new IOException(e);
                    }
                }, stackBuffer);
            }
        }
        return true;
    }

    @Override
    public boolean rangeScan(byte[] cacheId, byte[] fromInclusive, byte[] toExclusive, KeyValueStream stream) throws Exception {
        LOG.warn("Ignored range scan against filer cache key values");
        return true;
    }

    @Override
    public void put(byte[] cacheId,
        byte[][] keys,
        byte[][] values,
        boolean commitOnUpdate,
        boolean fsyncOnCommit,
        StackBuffer stackBuffer) throws Exception {

        byte[][][] powerKeyBytes = new byte[powerIndex.length][][];
        for (int i = 0; i < keys.length; i++) {
            byte[] keyBytes = keys[i];
            int power = FilerIO.chunkPower(keyBytes.length, 0);
            if (powerKeyBytes[power] == null) {
                powerKeyBytes[power] = new byte[keys.length][];
            }
            powerKeyBytes[power][i] = keyBytes;
        }

        for (int power = 0; power < powerKeyBytes.length; power++) {
            long expectedKeySize = FilerIO.chunkLength(power);
            byte[][] keyBytes = powerKeyBytes[power];
            if (keyBytes != null) {
                powerIndex[power].readWriteAutoGrow(cacheId,
                    keys.length, // lazy
                    (MapContext monkey, ChunkFiler chunkFiler, StackBuffer stackBuffer1, Object lock) -> {
                        if (monkey.keySize != expectedKeySize) {
                            throw new IllegalStateException("provided " + monkey.keySize + " expected " + expectedKeySize);
                        }
                        synchronized (lock) {
                            for (int i = 0; i < keyBytes.length; i++) {
                                if (keyBytes[i] != null) {
                                    MapStore.INSTANCE.add(chunkFiler, monkey, (byte) 1, keyBytes[i], values[i], stackBuffer1);
                                }
                            }
                            return null;
                        }
                    },
                    stackBuffer);
            }
        }
    }
}
