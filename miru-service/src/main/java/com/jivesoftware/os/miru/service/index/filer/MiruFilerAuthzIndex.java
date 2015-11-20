package com.jivesoftware.os.miru.service.index.filer;

import com.jivesoftware.os.filer.io.StripingLocksProvider;
import com.jivesoftware.os.filer.io.api.KeyedFilerStore;
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
    private final long indexId;
    private final KeyedFilerStore keyedStore;
    private final MiruAuthzCache<BM> cache;
    private final StripingLocksProvider<String> stripingLocksProvider;

    public MiruFilerAuthzIndex(MiruBitmaps<BM> bitmaps,
        long indexId,
        KeyedFilerStore keyedStore,
        MiruAuthzCache<BM> cache,
        StripingLocksProvider<String> stripingLocksProvider)
        throws Exception {

        this.bitmaps = bitmaps;
        this.indexId = indexId;
        this.keyedStore = keyedStore;
        this.cache = cache;
        this.stripingLocksProvider = stripingLocksProvider;

    }

    @Override
    public MiruInvertedIndex<BM> getAuthz(String authz) throws Exception {
        return new MiruFilerInvertedIndex<>(bitmaps, new MiruFieldIndex.IndexKey(indexId, MiruAuthzUtils.key(authz)), keyedStore, -1,
            stripingLocksProvider.lock(authz, 0));
    }

    @Override
    public BM getCompositeAuthz(MiruAuthzExpression authzExpression, byte[] primitiveBuffer) throws Exception {
        return cache.getOrCompose(authzExpression, authz -> getAuthz(authz).getIndex(primitiveBuffer).orNull());
    }

    @Override
    public void append(String authz, byte[] primitiveBuffer, int... ids) throws Exception {
        getAuthz(authz).append(primitiveBuffer, ids);
    }

    @Override
    public void set(String authz, byte[] primitiveBuffer, int... ids) throws Exception {
        getAuthz(authz).set(primitiveBuffer, ids);
        cache.increment(authz);
    }

    @Override
    public void remove(String authz, int id, byte[] primitiveBuffer) throws Exception {
        getAuthz(authz).remove(id, primitiveBuffer);
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
