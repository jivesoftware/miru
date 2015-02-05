package com.jivesoftware.os.miru.service.index.disk;

import com.google.common.base.Optional;
import com.google.common.cache.Cache;
import com.jivesoftware.os.filer.io.StripingLocksProvider;
import com.jivesoftware.os.filer.map.store.api.KeyedFilerStore;
import com.jivesoftware.os.miru.api.query.filter.MiruAuthzExpression;
import com.jivesoftware.os.miru.plugin.bitmap.MiruBitmaps;
import com.jivesoftware.os.miru.plugin.index.MiruAuthzIndex;
import com.jivesoftware.os.miru.plugin.index.MiruFieldIndex;
import com.jivesoftware.os.miru.plugin.index.MiruInvertedIndex;
import com.jivesoftware.os.miru.service.index.auth.MiruAuthzCache;
import com.jivesoftware.os.miru.service.index.auth.MiruAuthzUtils;

/** @author jonathan */
public class MiruFilerAuthzIndex<BM> implements MiruAuthzIndex<BM> {

    private final MiruBitmaps<BM> bitmaps;
    private final Cache<MiruFieldIndex.IndexKey, Optional<?>> fieldIndexCache;
    private final long indexId;
    private final KeyedFilerStore keyedStore;
    private final MiruAuthzCache<BM> cache;
    private final StripingLocksProvider<String> stripingLocksProvider;

    public MiruFilerAuthzIndex(MiruBitmaps<BM> bitmaps,
        Cache<MiruFieldIndex.IndexKey, Optional<?>> fieldIndexCache,
        long indexId,
        KeyedFilerStore keyedStore,
        MiruAuthzCache<BM> cache,
        StripingLocksProvider<String> stripingLocksProvider)
        throws Exception {

        this.bitmaps = bitmaps;
        this.fieldIndexCache = fieldIndexCache;
        this.indexId = indexId;
        this.keyedStore = keyedStore;
        this.cache = cache;
        this.stripingLocksProvider = stripingLocksProvider;

    }

    @Override
    public void append(String authz, int id) throws Exception {
        get(authz).append(id);
    }

    @Override
    public void set(String authz, int id) throws Exception {
        get(authz).set(id);
        cache.increment(authz);
    }

    @Override
    public void remove(String authz, int id) throws Exception {
        get(authz).remove(id);
        cache.increment(authz);
    }

    @Override
    public void close() {
        keyedStore.close();
        cache.clear();
    }

    private MiruInvertedIndex<BM> get(String authz) throws Exception {
        return new MiruFilerInvertedIndex<>(bitmaps, fieldIndexCache, new MiruFieldIndex.IndexKey(indexId, MiruAuthzUtils.key(authz)), keyedStore, -1,
            stripingLocksProvider.lock(authz));
    }

    @Override
    public BM getCompositeAuthz(MiruAuthzExpression authzExpression) throws Exception {
        return cache.getOrCompose(authzExpression, new MiruAuthzUtils.IndexRetriever<BM>() {
            @Override
            public BM getIndex(String authz) throws Exception {
                return get(authz).getIndex().orNull();
            }
        });
    }
}
