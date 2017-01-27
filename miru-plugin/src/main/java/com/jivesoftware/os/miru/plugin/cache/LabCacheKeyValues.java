package com.jivesoftware.os.miru.plugin.cache;

import com.google.common.base.Preconditions;
import com.google.common.primitives.Bytes;
import com.jivesoftware.os.filer.io.api.StackBuffer;
import com.jivesoftware.os.jive.utils.ordered.id.OrderIdProvider;
import com.jivesoftware.os.lab.api.ValueIndex;
import com.jivesoftware.os.lab.io.BolBuffer;
import com.jivesoftware.os.miru.plugin.cache.MiruPluginCacheProvider.CacheKeyValues;
import com.jivesoftware.os.miru.plugin.cache.MiruPluginCacheProvider.IndexKeyValueStream;
import com.jivesoftware.os.miru.plugin.cache.MiruPluginCacheProvider.KeyValueStream;
import com.jivesoftware.os.miru.plugin.index.MiruTermComposer;
import java.nio.ByteBuffer;
import java.util.Arrays;

/**
 *
 */
public class LabCacheKeyValues implements CacheKeyValues {

    private final String name;
    private final OrderIdProvider idProvider;
    private final ValueIndex<byte[]>[] indexes;

    public LabCacheKeyValues(String name, OrderIdProvider idProvider, ValueIndex<byte[]>[] indexes) {
        this.name = name;
        this.idProvider = idProvider;
        this.indexes = indexes;
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public boolean get(byte[] cacheId, byte[][] keys, IndexKeyValueStream stream, StackBuffer stackBuffer) throws Exception {
        int stripe = stripe(cacheId);
        byte[] prefixBytes = {(byte) cacheId.length};

        byte[][] keyBytes = new byte[keys.length][];
        for (int i = 0; i < keys.length; i++) {
            if (keys[i] != null) {
                keyBytes[i] = Bytes.concat(prefixBytes, cacheId, keys[i]);
            }
        }

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
                return stream.stream(index, tombstoned ? null : payload.asByteBuffer());
            },
            true
        );
        return true;
    }

    @Override
    public boolean rangeScan(byte[] cacheId, byte[] fromInclusive, byte[] toExclusive, KeyValueStream stream) throws Exception {
        Preconditions.checkArgument(cacheId.length <= Byte.MAX_VALUE, "Max cacheId length is " + Byte.MAX_VALUE);

        int stripe = stripe(cacheId);
        byte[] prefixBytes = {(byte) cacheId.length};
        byte[] fromKeyBytes = fromInclusive == null ? Bytes.concat(prefixBytes, cacheId) : Bytes.concat(prefixBytes, cacheId, fromInclusive);
        byte[] toKeyBytes;
        if (toExclusive == null) {
            toKeyBytes = Arrays.copyOf(fromKeyBytes, fromKeyBytes.length);
            MiruTermComposer.makeUpperExclusive(toKeyBytes);
        } else {
            toKeyBytes = Bytes.concat(prefixBytes, cacheId, toExclusive);
        }

        return indexes[stripe].rangeScan(fromKeyBytes,
            toKeyBytes,
            (index, key, timestamp, tombstoned, version, payload) -> {
                if (tombstoned) {
                    return true; //TODO reconsider
                } else {
                    ByteBuffer bbkey = key.asByteBuffer();
                    bbkey.position(1 + cacheId.length);
                    ByteBuffer cacheKey = bbkey.slice();
                    return stream.stream(cacheKey, payload == null ? null : payload.asByteBuffer());
                }
            },
            true
        );
    }

    @Override
    public void put(byte[] cacheId,
        byte[][] keys,
        byte[][] values,
        boolean commitOnUpdate,
        boolean fsyncOnCommit,
        StackBuffer stackBuffer) throws Exception {

        int stripe = stripe(cacheId);
        byte[] prefixBytes = {(byte) cacheId.length};

        byte[][] keyBytes = new byte[keys.length][];
        for (int i = 0; i < keys.length; i++) {
            if (keys[i] != null) {
                keyBytes[i] = Bytes.concat(prefixBytes, cacheId, keys[i]);
            }
        }

        BolBuffer entryBuffer = new BolBuffer();
        BolBuffer keyBuffer = new BolBuffer();
        long timestamp = System.currentTimeMillis();
        long version = idProvider.nextId();
        indexes[stripe].append(stream -> {
            for (int i = 0; i < keyBytes.length; i++) {
                byte[] key = keyBytes[i];
                if (key != null) {
                    stream.stream(-1, key, timestamp, false, version, values[i]);
                }
            }
            return true;
        }, fsyncOnCommit, entryBuffer, keyBuffer);

        if (commitOnUpdate) {
            indexes[stripe].commit(fsyncOnCommit, true);
        }
    }

    public void commit(boolean fsyncOnCommit) throws Exception {
        for (ValueIndex<byte[]> index : indexes) {
            index.commit(fsyncOnCommit, true);
        }
    }

    public void close(boolean flushUncommited, boolean fsync) throws Exception {
        for (ValueIndex<byte[]> index : indexes) {
            index.close(flushUncommited, fsync);
        }
    }

    private int stripe(byte[] cacheId) {
        return Math.abs(compute(cacheId, 0, cacheId.length) % indexes.length);
    }

    private int compute(byte[] bytes, int offset, int length) {
        int hash = 0;
        long randMult = 0x5_DEEC_E66DL;
        long randAdd = 0xBL;
        long randMask = (1L << 48) - 1;
        long seed = bytes.length;
        for (int i = offset; i < offset + length; i++) {
            long x = (seed * randMult + randAdd) & randMask;

            seed = x;
            hash += (bytes[i] + 128) * x;
        }
        return hash;
    }
}
