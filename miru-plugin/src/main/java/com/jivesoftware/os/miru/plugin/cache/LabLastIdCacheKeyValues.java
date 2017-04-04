package com.jivesoftware.os.miru.plugin.cache;

import com.google.common.primitives.Bytes;
import com.jivesoftware.os.filer.io.api.StackBuffer;
import com.jivesoftware.os.jive.utils.ordered.id.OrderIdProvider;
import com.jivesoftware.os.lab.api.ValueIndex;
import com.jivesoftware.os.lab.io.BolBuffer;
import com.jivesoftware.os.miru.plugin.cache.MiruPluginCacheProvider.ConsumeLastIdKeyValueStream;
import com.jivesoftware.os.miru.plugin.cache.MiruPluginCacheProvider.LastIdCacheKeyValues;
import com.jivesoftware.os.miru.plugin.cache.MiruPluginCacheProvider.LastIdIndexKeyValueStream;

/**
 *
 */
public class LabLastIdCacheKeyValues implements LastIdCacheKeyValues {

    private final String name;
    private final OrderIdProvider idProvider;
    private final ValueIndex<byte[]>[] indexes;

    public LabLastIdCacheKeyValues(String name, OrderIdProvider idProvider, ValueIndex<byte[]>[] indexes) {
        this.name = name;
        this.idProvider = idProvider;
        this.indexes = indexes;
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public boolean get(byte[] cacheId, byte[][] keys, LastIdIndexKeyValueStream stream, StackBuffer stackBuffer) throws Exception {
        int stripe = stripe(cacheId);
        byte[] prefixBytes = { (byte) cacheId.length };

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
                return stream.stream(index, tombstoned ? null : payload == null ? null : payload.asByteBuffer(), (int) timestamp);
            }, true);
        return true;
    }

    /*@Override
    public boolean rangeScan(byte[] cacheId, byte[] fromInclusive, byte[] toExclusive, LastIdKeyValueStream stream) throws Exception {
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

        return indexes[stripe].rangeScan(fromKeyBytes, toKeyBytes, (index, key, timestamp, tombstoned, version, payload) -> {
            if (tombstoned) {
                return true; //TODO reconsider
            } else {
                ByteBuffer bbKey = key.asByteBuffer();
                bbKey.clear();
                bbKey.position(1 + cacheId.length);
                ByteBuffer cacheKey = bbKey.slice();
                return stream.stream(cacheKey, payload == null ? null : payload.asByteBuffer(), (int) timestamp);
            }
        }, true);
    }*/

    @Override
    public boolean put(byte[] cacheId,
        boolean commitOnUpdate,
        boolean fsyncOnCommit,
        ConsumeLastIdKeyValueStream consume,
        StackBuffer stackBuffer) throws Exception {

        int stripe = stripe(cacheId);
        byte[] prefixBytes = { (byte) cacheId.length };
        BolBuffer entryBuffer = new BolBuffer();
        BolBuffer keyBuffer = new BolBuffer();
        long version = idProvider.nextId();
        boolean result = indexes[stripe].append(stream -> {
            return consume.consume((key, value, timestamp) -> {
                byte[] keyBytes = Bytes.concat(prefixBytes, cacheId, key);
                return stream.stream(-1, keyBytes, timestamp, false, version, value);
            });
        }, fsyncOnCommit, entryBuffer, keyBuffer);

        if (commitOnUpdate) {
            indexes[stripe].commit(fsyncOnCommit, true);
        }

        return result;
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

    public void compact(boolean fsync, int minDebt, int maxDebt, boolean waitIfTooFarBehind) throws Exception {
        for (ValueIndex<byte[]> index : indexes) {
            index.compact(fsync, minDebt, maxDebt, waitIfTooFarBehind);
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
