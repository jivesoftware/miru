package com.jivesoftware.os.miru.service.stream.cache;

import com.google.common.primitives.Bytes;
import com.jivesoftware.os.filer.io.ByteArrayStripingLocksProvider;
import com.jivesoftware.os.filer.io.api.StackBuffer;
import com.jivesoftware.os.jive.utils.ordered.id.OrderIdProvider;
import com.jivesoftware.os.lab.api.ValueIndex;
import com.jivesoftware.os.lab.io.BolBuffer;
import com.jivesoftware.os.lab.io.api.UIO;
import com.jivesoftware.os.miru.api.base.MiruStreamId;
import com.jivesoftware.os.miru.plugin.bitmap.MiruBitmaps;
import com.jivesoftware.os.miru.plugin.cache.MiruPluginCacheProvider.CacheKeyBitmaps;
import com.jivesoftware.os.miru.plugin.index.BitmapAndLastId;
import com.jivesoftware.os.miru.plugin.partition.TrackError;
import com.jivesoftware.os.miru.service.index.lab.LabInvertedIndex;
import java.util.Arrays;

/**
 *
 */
public class LabCacheKeyBitmaps<BM extends IBM, IBM> implements CacheKeyBitmaps<BM, IBM> {

    private static final int LAST_ID_PREFIX = 0xFFFF;
    private static final byte[] LAST_ID_BYTES = new byte[] { -1, -1 };

    private final String name;
    private final OrderIdProvider idProvider;
    private final MiruBitmaps<BM, IBM> bitmaps;
    private final TrackError trackError;
    private final boolean atomized;
    private final ValueIndex<byte[]>[] stores;
    private final ByteArrayStripingLocksProvider stripingLocksProvider;

    public LabCacheKeyBitmaps(String name,
        OrderIdProvider idProvider,
        MiruBitmaps<BM, IBM> bitmaps,
        TrackError trackError,
        boolean atomized,
        ValueIndex<byte[]>[] stores,
        ByteArrayStripingLocksProvider stripingLocksProvider) {
        this.name = name;
        this.idProvider = idProvider;
        this.bitmaps = bitmaps;
        this.trackError = trackError;
        this.atomized = atomized;
        this.stores = stores;
        this.stripingLocksProvider = stripingLocksProvider;
    }

    @Override
    public String name() {
        return name;
    }

    private LabInvertedIndex<BM, IBM> getIndex(byte[] cacheId) throws Exception {
        return new LabInvertedIndex<>(idProvider,
            bitmaps,
            trackError,
            name,
            -1,
            atomized,
            bitmapIndexKey(cacheId),
            getStore(cacheId),
            null,
            null,
            stripingLocksProvider.lock(cacheId, 0));
    }

    private byte[] bitmapIndexKey(byte[] cacheId) {
        if (cacheId.length >= LAST_ID_PREFIX) {
            throw new IllegalArgumentException("Bad cacheId length");
        }
        if (atomized) {
            byte[] cacheIdLength = new byte[2];
            UIO.shortBytes((short) (cacheId.length & 0xFFFF), cacheIdLength, 0);
            return Bytes.concat(cacheIdLength, cacheId);
        } else {
            return cacheId;
        }
    }

    private ValueIndex<byte[]> getStore(byte[] cacheId) {
        return stores[stripe(cacheId)];
    }

    @Override
    public BM get(byte[] cacheId, StackBuffer stackBuffer) throws Exception {
        BitmapAndLastId<BM> bitmapAndLastId = new BitmapAndLastId<>();
        getIndex(cacheId).getIndex(bitmapAndLastId, stackBuffer);
        return bitmapAndLastId.getBitmap();
    }

    @Override
    public boolean or(byte[] cacheId, IBM bitmap, StackBuffer stackBuffer) throws Exception {
        getIndex(cacheId).or(bitmap, stackBuffer);
        return true;
    }

    @Override
    public boolean andNot(byte[] cacheId, IBM bitmap, StackBuffer stackBuffer) throws Exception {
        getIndex(cacheId).andNot(bitmap, stackBuffer);
        return true;
    }

    private byte[] lastIdKey(byte[] cacheId) {
        byte[] key = new byte[LAST_ID_BYTES.length + cacheId.length];
        System.arraycopy(LAST_ID_BYTES, 0, key, 0, LAST_ID_BYTES.length);
        System.arraycopy(cacheId, 0, key, LAST_ID_BYTES.length, cacheId.length);
        return key;
    }

    @Override
    public int getLastId(byte[] cacheId) throws Exception {
        ValueIndex<byte[]> store = getStore(cacheId);
        int[] lastId = { -1 };
        byte[] key = lastIdKey(cacheId);
        store.get(keyStream -> keyStream.key(0, key, 0, key.length),
            (index, key1, timestamp, tombstoned, version, payload) -> {
                lastId[0] = (int) timestamp;
                return true;
            },
            false);
        return lastId[0];
    }

    @Override
    public void setLastId(byte[] cacheId, int lastId) throws Exception {
        ValueIndex<byte[]> store = getStore(cacheId);
        byte[] key = lastIdKey(cacheId);
        long version = idProvider.nextId();
        store.append(stream -> stream.stream(0, key, lastId, false, version, new byte[0]),
            false,
            new BolBuffer(),
            new BolBuffer());
    }

    public void commit(boolean fsyncOnCommit) throws Exception {
        for (ValueIndex<byte[]> index : stores) {
            index.commit(fsyncOnCommit, true);
        }
    }

    public void close(boolean flushUncommited, boolean fsync) throws Exception {
        for (ValueIndex<byte[]> index : stores) {
            index.close(flushUncommited, fsync);
        }
    }

    public void compact(boolean fsync, int minDebt, int maxDebt, boolean waitIfTooFarBehind) throws Exception {
        for (ValueIndex<byte[]> index : stores) {
            index.compact(fsync, minDebt, maxDebt, waitIfTooFarBehind);
        }
    }

    private int stripe(byte[] cacheId) {
        return Math.abs(compute(cacheId, 0, cacheId.length) % stores.length);
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