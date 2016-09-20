package com.jivesoftware.os.miru.service.index.filer;

import com.jivesoftware.os.filer.io.StripingLocksProvider;
import com.jivesoftware.os.filer.io.api.KeyedFilerStore;
import com.jivesoftware.os.filer.io.api.StackBuffer;
import com.jivesoftware.os.miru.api.query.filter.MiruAuthzExpression;
import com.jivesoftware.os.miru.plugin.bitmap.MiruBitmaps;
import com.jivesoftware.os.miru.plugin.index.BitmapAndLastId;
import com.jivesoftware.os.miru.plugin.index.MiruAuthzIndex;
import com.jivesoftware.os.miru.plugin.index.MiruInvertedIndex;
import com.jivesoftware.os.miru.plugin.partition.TrackError;
import com.jivesoftware.os.miru.service.index.auth.MiruAuthzCache;
import com.jivesoftware.os.miru.service.index.auth.MiruAuthzUtils;

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
        return new MiruFilerInvertedIndex<>(bitmaps, trackError, "authz", -3, MiruAuthzUtils.key(authz), keyedStore, stripingLocksProvider.lock(authz, 0));
    }

    @Override
    public BM getCompositeAuthz(MiruAuthzExpression authzExpression, StackBuffer stackBuffer) throws Exception {
        return cache.getOrCompose(authzExpression, authz -> {
            BitmapAndLastId<BM> container = new BitmapAndLastId<>();
            getAuthz(authz).getIndex(container, stackBuffer);
            return container.getBitmap();
        });
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
    public void close() {
        keyedStore.close();
        cache.clear();
    }

}
