package com.jivesoftware.os.miru.service.index.lab;

import com.google.common.primitives.Bytes;
import com.jivesoftware.os.filer.io.StripingLocksProvider;
import com.jivesoftware.os.filer.io.api.StackBuffer;
import com.jivesoftware.os.jive.utils.ordered.id.OrderIdProvider;
import com.jivesoftware.os.lab.api.ValueIndex;
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
    private final ValueIndex[] keyedStores;
    private final MiruAuthzCache<BM, IBM> cache;
    private final StripingLocksProvider<String> stripingLocksProvider;

    public LabAuthzIndex(OrderIdProvider idProvider,
        MiruBitmaps<BM, IBM> bitmaps,
        TrackError trackError,
        byte[] prefix,
        ValueIndex[] keyedStores,
        MiruAuthzCache<BM, IBM> cache,
        StripingLocksProvider<String> stripingLocksProvider)
        throws Exception {

        this.idProvider = idProvider;
        this.bitmaps = bitmaps;
        this.trackError = trackError;
        this.prefix = prefix;
        this.keyedStores = keyedStores;
        this.cache = cache;
        this.stripingLocksProvider = stripingLocksProvider;

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
            Bytes.concat(prefix, MiruAuthzUtils.key(authz)),
            getStore(authz),
            stripingLocksProvider.lock(authz, 0));
    }

    @Override
    public BM getCompositeAuthz(MiruAuthzExpression authzExpression, StackBuffer stackBuffer) throws Exception {
        return cache.getOrCompose(authzExpression, authz -> getAuthz(authz).getIndex(stackBuffer).orNull());
    }

    @Override
    public void append(String authz, StackBuffer stackBuffer, int... ids) throws Exception {
        getAuthz(authz).append(stackBuffer, ids);
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
        for (ValueIndex keyedStore : keyedStores) {
            keyedStore.close();
        }
        cache.clear();
    }

}
