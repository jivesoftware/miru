package com.jivesoftware.os.miru.plugin.cache;

import com.google.common.primitives.Bytes;
import com.jivesoftware.os.filer.io.api.StackBuffer;
import com.jivesoftware.os.jive.utils.ordered.id.OrderIdProvider;
import com.jivesoftware.os.lab.api.ValueIndex;
import com.jivesoftware.os.miru.plugin.cache.MiruPluginCacheProvider.CacheKeyValues;
import com.jivesoftware.os.miru.plugin.cache.MiruPluginCacheProvider.GetKeyValueStream;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

/**
 *
 */
public class LabCacheKeyValues implements CacheKeyValues {

    private final OrderIdProvider idProvider;
    private final ValueIndex[] indexes;
    private final boolean fsyncOnCommit;

    public LabCacheKeyValues(OrderIdProvider idProvider, ValueIndex[] indexes, boolean fsyncOnCommit) {
        this.idProvider = idProvider;
        this.indexes = indexes;
        this.fsyncOnCommit = fsyncOnCommit;
    }

    @Override
    public boolean get(String cacheId, byte[][] keys, GetKeyValueStream stream, StackBuffer stackBuffer) throws Exception {
        byte[] cacheIdBytes = cacheId.getBytes(StandardCharsets.UTF_8);
        byte[][][] stripeKeyBytes = new byte[indexes.length][][];
        for (int i = 0; i < keys.length; i++) {
            if (keys[i] != null) {
                byte[] keyBytes = Bytes.concat(cacheIdBytes, keys[i]);

                int stripe = Math.abs(Arrays.hashCode(keyBytes) % indexes.length);
                if (stripeKeyBytes[stripe] == null) {
                    stripeKeyBytes[stripe] = new byte[keys.length][];
                }
                stripeKeyBytes[stripe][i] = keyBytes;
            }
        }

        for (int stripe = 0; stripe < stripeKeyBytes.length; stripe++) {
            byte[][] keyBytes = stripeKeyBytes[stripe];
            if (keyBytes != null) {
                indexes[stripe].get(
                    keyStream -> {
                        for (int i = 0; i < keyBytes.length; i++) {
                            byte[] key = keyBytes[i];
                            if (key != null) {
                                if (!keyStream.key(i, key, 0, key.length)) {
                                    return false;
                                }
                            }
                        }
                        return true;
                    },
                    (index, key, timestamp, tombstoned, version, payload) -> {
                        return stream.stream(index, keys[index], tombstoned ? null : payload);
                    });
            }
        }
        return true;
    }

    @Override
    public void put(String cacheId, byte[][] keys, byte[][] values, StackBuffer stackBuffer) throws Exception {
        byte[] cacheIdBytes = cacheId.getBytes(StandardCharsets.UTF_8);
        byte[][][] stripeKeyBytes = new byte[indexes.length][][];
        for (int i = 0; i < keys.length; i++) {
            if (keys[i] != null) {
                byte[] keyBytes = Bytes.concat(cacheIdBytes, keys[i]);

                int stripe = Math.abs(Arrays.hashCode(keyBytes) % indexes.length);
                if (stripeKeyBytes[stripe] == null) {
                    stripeKeyBytes[stripe] = new byte[keys.length][];
                }
                stripeKeyBytes[stripe][i] = keyBytes;
            }
        }

        long timestamp = System.currentTimeMillis();
        long version = idProvider.nextId();
        for (int stripe = 0; stripe < stripeKeyBytes.length; stripe++) {
            byte[][] keysBytes = stripeKeyBytes[stripe];
            if (keysBytes != null) {
                indexes[stripe].append(stream -> {
                    for (int i = 0; i < keysBytes.length; i++) {
                        byte[] keyBytes = keysBytes[i];
                        if (keyBytes != null) {
                            stream.stream(-1, keyBytes, timestamp, false, version, values[i]);
                        }
                    }
                    return true;
                }, fsyncOnCommit);

                //TODO consider making this a lazy commit
                indexes[stripe].commit(fsyncOnCommit);
            }
        }
    }
}
