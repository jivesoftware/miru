package com.jivesoftware.os.miru.service.index.disk;

import com.google.common.base.Optional;
import com.jivesoftware.os.filer.io.StripingLocksProvider;
import com.jivesoftware.os.filer.keyed.store.KeyedFilerStore;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.api.query.filter.MiruAuthzExpression;
import com.jivesoftware.os.miru.plugin.bitmap.MiruBitmaps;
import com.jivesoftware.os.miru.plugin.index.MiruAuthzIndex;
import com.jivesoftware.os.miru.plugin.index.MiruInvertedIndex;
import com.jivesoftware.os.miru.service.index.BulkExport;
import com.jivesoftware.os.miru.service.index.BulkImport;
import com.jivesoftware.os.miru.service.index.SimpleBulkExport;
import com.jivesoftware.os.miru.service.index.auth.MiruAuthzCache;
import com.jivesoftware.os.miru.service.index.auth.MiruAuthzUtils;
import com.jivesoftware.os.miru.service.index.memory.KeyedInvertedIndexStream;
import java.io.IOException;

/** @author jonathan */
public class MiruFilerAuthzIndex<BM> implements MiruAuthzIndex<BM>,
    BulkExport<Void, KeyedInvertedIndexStream<BM>>,
    BulkImport<Void, KeyedInvertedIndexStream<BM>> {

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

    @Override
    public Void bulkExport(MiruTenantId tenantId, KeyedInvertedIndexStream<BM> callback) throws Exception {
        //TODO NEVER
        return null;
    }

    @Override
    public void bulkImport(final MiruTenantId tenantId, BulkExport<Void, KeyedInvertedIndexStream<BM>> export) throws Exception {
        export.bulkExport(tenantId, new KeyedInvertedIndexStream<BM>() {
            @Override
            public boolean stream(byte[] key, MiruInvertedIndex<BM> importIndex) throws IOException {
                if (key == null) {
                    return true;
                }
                try {
                    Optional<BM> index = importIndex.getIndex();
                    if (index.isPresent()) {
                        MiruFilerInvertedIndex<BM> invertedIndex = new MiruFilerInvertedIndex<>(
                            bitmaps, keyedStore, key, -1, new Object());
                        invertedIndex.bulkImport(tenantId, new SimpleBulkExport<>(importIndex));
                    }
                    return true;
                } catch (Exception e) {
                    throw new IOException("Failed to stream import", e);
                }
            }
        });
    }
}
