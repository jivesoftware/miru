package com.jivesoftware.os.miru.service.index.disk;

import com.jivesoftware.os.filer.io.StripingLocksProvider;
import com.jivesoftware.os.filer.map.store.api.KeyedFilerStore;
import com.jivesoftware.os.miru.api.query.filter.MiruAuthzExpression;
import com.jivesoftware.os.miru.plugin.bitmap.MiruBitmaps;
import com.jivesoftware.os.miru.plugin.index.MiruAuthzIndex;
import com.jivesoftware.os.miru.plugin.index.MiruInvertedIndex;
import com.jivesoftware.os.miru.service.index.auth.MiruAuthzCache;
import com.jivesoftware.os.miru.service.index.auth.MiruAuthzUtils;

/** @author jonathan */
public class MiruFilerAuthzIndex<BM> implements MiruAuthzIndex<BM> {

    private final MiruBitmaps<BM> bitmaps;
    private final KeyedFilerStore keyedStore;
    private final MiruAuthzCache<BM> cache;
    private final StripingLocksProvider<String> stripingLocksProvider;

    public MiruFilerAuthzIndex(MiruBitmaps<BM> bitmaps,
        KeyedFilerStore keyedStore,
        MiruAuthzCache<BM> cache,
        StripingLocksProvider<String> stripingLocksProvider)
        throws Exception {

        this.bitmaps = bitmaps;
        this.keyedStore = keyedStore;
        this.cache = cache;
        this.stripingLocksProvider = stripingLocksProvider;

    }

    @Override
    public void index(String authz, int id) throws Exception {
        get(authz).append(id);
    }

    @Override
    public void repair(String authz, int id, boolean value) throws Exception {
        MiruInvertedIndex index = get(authz);
        if (value) {
            index.set(id);
        } else {
            index.remove(id);
        }
        cache.increment(authz);
    }

    @Override
    public void close() {
        keyedStore.close();
        cache.clear();
    }

    private MiruInvertedIndex<BM> get(String authz) throws Exception {
        return new MiruFilerInvertedIndex<>(bitmaps, keyedStore, MiruAuthzUtils.key(authz), -1, stripingLocksProvider.lock(authz));
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
