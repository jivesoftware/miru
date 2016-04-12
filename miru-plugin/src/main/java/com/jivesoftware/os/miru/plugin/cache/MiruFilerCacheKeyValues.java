package com.jivesoftware.os.miru.plugin.cache;

import com.jivesoftware.os.filer.io.FilerIO;
import com.jivesoftware.os.filer.io.api.KeyedFilerStore;
import com.jivesoftware.os.filer.io.api.StackBuffer;
import com.jivesoftware.os.filer.io.chunk.ChunkFiler;
import com.jivesoftware.os.filer.io.map.MapContext;
import com.jivesoftware.os.filer.io.map.MapStore;
import com.jivesoftware.os.miru.plugin.cache.MiruPluginCacheProvider.CacheKeyValues;
import com.jivesoftware.os.miru.plugin.cache.MiruPluginCacheProvider.GetKeyValueStream;
import java.nio.charset.StandardCharsets;

/**
 *
 */
public class MiruFilerCacheKeyValues implements CacheKeyValues {

    private final KeyedFilerStore<Integer, MapContext>[] powerIndex;

    public MiruFilerCacheKeyValues(KeyedFilerStore<Integer, MapContext>[] powerIndex) {
        this.powerIndex = powerIndex;
    }

    @Override
    public boolean get(String cacheId, byte[][] keys, GetKeyValueStream stream, StackBuffer stackBuffer) throws Exception {
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

        byte[] cacheIdBytes = cacheId.getBytes(StandardCharsets.UTF_8);
        for (int power = 0; power < powerKeyBytes.length; power++) {
            long expectedKeySize = FilerIO.chunkLength(power);

            byte[][] keyBytes = powerKeyBytes[power];
            if (keyBytes != null) {
                powerIndex[power].read(cacheIdBytes, null, (monkey, filer, _stackBuffer, lock) -> {
                    if (filer != null) {
                        if (monkey.keySize != expectedKeySize) {
                            throw new IllegalStateException("provided " + monkey.keySize + " expected " + expectedKeySize);
                        }
                        synchronized (lock) {
                            for (int i = 0; i < keyBytes.length; i++) {
                                if (keyBytes[i] != null) {
                                    byte[] payload = MapStore.INSTANCE.getPayload(filer, monkey, keyBytes[i], _stackBuffer);
                                    if (!stream.stream(i, keyBytes[i], payload)) {
                                        return null;
                                    }
                                }
                            }
                        }
                    } else {
                        for (int i = 0; i < keyBytes.length; i++) {
                            if (keyBytes[i] != null) {
                                if (!stream.stream(i, keyBytes[i], null)) {
                                    return null;
                                }
                            }
                        }
                    }
                    return null;
                }, stackBuffer);
            }
        }
        return true;
    }

    @Override
    public void put(String cacheId,
        byte[][] keys,
        byte[][] values,
        boolean commitOnUpdate,
        boolean fsyncOnCommit,
        StackBuffer stackBuffer) throws Exception {

        byte[] cacheIdBytes = cacheId.getBytes(StandardCharsets.UTF_8);

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
                powerIndex[power].readWriteAutoGrow(cacheIdBytes,
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
