package com.jivesoftware.os.miru.service.index.lab;

import com.google.common.primitives.Bytes;
import com.jivesoftware.os.filer.io.StripingLocksProvider;
import com.jivesoftware.os.filer.io.api.StackBuffer;
import com.jivesoftware.os.jive.utils.ordered.id.OrderIdProvider;
import com.jivesoftware.os.lab.api.ValueIndex;
import com.jivesoftware.os.lab.io.api.UIO;
import com.jivesoftware.os.miru.api.query.filter.MiruAuthzExpression;
import com.jivesoftware.os.miru.plugin.bitmap.MiruBitmaps;
import com.jivesoftware.os.miru.plugin.index.MiruAuthzIndex;
import com.jivesoftware.os.miru.plugin.index.MiruInvertedIndex;
import com.jivesoftware.os.miru.plugin.partition.TrackError;
import com.jivesoftware.os.miru.service.index.auth.MiruAuthzCache;
import com.jivesoftware.os.miru.service.index.auth.MiruAuthzUtils;

/** @author jonathan */
public class LabAuthzIndex<BM extends IBM, IBM> implements MiruAuthzIndex<BM, IBM> {

    private final OrderIdProvider idProvider;
    private final MiruBitmaps<BM, IBM> bitmaps;
    private final TrackError trackError;
    private final byte[] prefix;
    private final boolean atomized;
    private final ValueIndex[] keyedStores;
    private final MiruAuthzCache<BM, IBM> cache;
    private final StripingLocksProvider<String> stripingLocksProvider;
    private final long labFieldDeltaMaxCardinality;

    public LabAuthzIndex(OrderIdProvider idProvider,
        MiruBitmaps<BM, IBM> bitmaps,
        TrackError trackError,
        byte[] prefix,
        boolean atomized,
        ValueIndex[] keyedStores,
        MiruAuthzCache<BM, IBM> cache,
        StripingLocksProvider<String> stripingLocksProvider,
        long labFieldDeltaMaxCardinality)
        throws Exception {

        this.idProvider = idProvider;
        this.bitmaps = bitmaps;
        this.trackError = trackError;
        this.prefix = prefix;
        this.atomized = atomized;
        this.keyedStores = keyedStores;
        this.cache = cache;
        this.stripingLocksProvider = stripingLocksProvider;

        this.labFieldDeltaMaxCardinality = labFieldDeltaMaxCardinality;
    }

    private ValueIndex getStore(String authz) {
        return keyedStores[Math.abs(authz.hashCode() % keyedStores.length)];
    }

    @Override
    public MiruInvertedIndex<BM, IBM> getAuthz(String authz) throws Exception {
        return new LabInvertedIndex<>(idProvider,
            bitmaps,
            trackError,
            "authz",
            -3,
            atomized,
            bitmapIndexKey(MiruAuthzUtils.key(authz)),
            getStore(authz),
            null,
            null,
            stripingLocksProvider.lock(authz, 0),
            labFieldDeltaMaxCardinality);
    }

    private byte[] bitmapIndexKey(byte[] authzBytes) {
        if (atomized) {
            byte[] authzLength = new byte[2];
            UIO.shortBytes((short) (authzBytes.length & 0xFFFF), authzLength, 0);
            return Bytes.concat(prefix, authzLength, authzBytes);
        } else {
            return Bytes.concat(prefix, authzBytes);
        }
    }

    @Override
    public BM getCompositeAuthz(MiruAuthzExpression authzExpression, StackBuffer stackBuffer) throws Exception {
        return cache.getOrCompose(authzExpression, authz -> getAuthz(authz).getIndex(stackBuffer).orNull());
    }

    @Override
    public void set(String authz, StackBuffer stackBuffer, int... ids) throws Exception {
        getAuthz(authz).set(stackBuffer, ids);
        cache.increment(authz);
    }

    @Override
    public void remove(String authz, StackBuffer stackBuffer, int... ids) throws Exception {
        getAuthz(authz).remove(stackBuffer, ids);
        cache.increment(authz);
    }

    @Override
    public void close() throws Exception {
        cache.clear();
    }

}
