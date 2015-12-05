package com.jivesoftware.os.miru.service.index.filer;

import com.jivesoftware.os.filer.io.StripingLocksProvider;
import com.jivesoftware.os.filer.io.api.KeyedFilerStore;
import com.jivesoftware.os.filer.io.api.StackBuffer;
import com.jivesoftware.os.miru.api.query.filter.MiruAuthzExpression;
import com.jivesoftware.os.miru.plugin.bitmap.MiruBitmaps;
import com.jivesoftware.os.miru.plugin.index.MiruAuthzIndex;
import com.jivesoftware.os.miru.plugin.index.MiruInvertedIndex;
import com.jivesoftware.os.miru.service.index.auth.MiruAuthzCache;
import com.jivesoftware.os.miru.service.index.auth.MiruAuthzUtils;
import com.jivesoftware.os.miru.plugin.partition.TrackError;

/** @author jonathan */
public class MiruFilerAuthzIndex<BM extends IBM, IBM> implements MiruAuthzIndex<BM, IBM> {

    private final MiruBitmaps<BM, IBM> bitmaps;
    private final TrackError trackError;
    private final KeyedFilerStore<Long, Void> keyedStore;
    private final MiruAuthzCache<BM, IBM> cache;
    private final StripingLocksProvider<String> stripingLocksProvider;

    public MiruFilerAuthzIndex(MiruBitmaps<BM, IBM> bitmaps,
        TrackError trackError,
        KeyedFilerStore<Long, Void> keyedStore,
        MiruAuthzCache<BM, IBM> cache,
        StripingLocksProvider<String> stripingLocksProvider)
        throws Exception {

        this.bitmaps = bitmaps;
        this.trackError = trackError;
        this.keyedStore = keyedStore;
        this.cache = cache;
        this.stripingLocksProvider = stripingLocksProvider;

    }

    @Override
    public MiruInvertedIndex<BM, IBM> getAuthz(String authz) throws Exception {
        return new MiruFilerInvertedIndex<>(bitmaps, trackError, MiruAuthzUtils.key(authz), keyedStore, -1, stripingLocksProvider.lock(authz, 0));
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
    public void remove(String authz, int id, StackBuffer stackBuffer) throws Exception {
        getAuthz(authz).remove(id, stackBuffer);
        cache.increment(authz);
    }

    @Override
    public void close() {
        keyedStore.close();
        cache.clear();
    }

    private static final String IDEA_TYPE_CODE = "idea";
    public static final int IDEA_TYPE_ID = IDEA_TYPE_CODE.hashCode();

    public static void main(String[] args) {
        System.out.println("" + IDEA_TYPE_ID);
    }

}
