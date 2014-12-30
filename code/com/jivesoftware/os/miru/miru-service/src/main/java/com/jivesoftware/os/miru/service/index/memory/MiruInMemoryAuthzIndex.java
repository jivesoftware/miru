package com.jivesoftware.os.miru.service.index.memory;

import com.google.common.base.Charsets;
import com.jivesoftware.os.filer.map.store.api.KeyValueContext;
import com.jivesoftware.os.filer.map.store.api.KeyValueStore;
import com.jivesoftware.os.filer.map.store.api.KeyValueTransaction;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.api.query.filter.MiruAuthzExpression;
import com.jivesoftware.os.miru.plugin.bitmap.MiruBitmaps;
import com.jivesoftware.os.miru.plugin.index.MiruAuthzIndex;
import com.jivesoftware.os.miru.plugin.index.MiruInvertedIndex;
import com.jivesoftware.os.miru.service.index.BulkExport;
import com.jivesoftware.os.miru.service.index.BulkImport;
import com.jivesoftware.os.miru.service.index.auth.MiruAuthzCache;
import com.jivesoftware.os.miru.service.index.auth.MiruAuthzUtils;
import java.io.IOException;

/**
 * @author jonathan
 */
public class MiruInMemoryAuthzIndex<BM> implements MiruAuthzIndex<BM>,
    BulkImport<Void, KeyedInvertedIndexStream<BM>>,
    BulkExport<Void, KeyedInvertedIndexStream<BM>> {

    private final MiruBitmaps<BM> bitmaps;
    private final KeyValueStore<byte[], ReadWrite<BM>> store;
    private final MiruAuthzCache<BM> cache;

    public MiruInMemoryAuthzIndex(MiruBitmaps<BM> bitmaps, KeyValueStore<byte[], ReadWrite<BM>> store, MiruAuthzCache<BM> cache) {
        this.bitmaps = bitmaps;
        this.store = store;
        this.cache = cache;
    }

    @Override
    public long sizeInMemory() throws Exception {
        return 0;
    }

    @Override
    public long sizeOnDisk() throws Exception {
        return 0;
    }

    @Override
    public void index(String authz, int id) throws Exception {
        getOrAllocate(authz).append(id);
        cache.increment(authz);
    }

    @Override
    public void repair(String authz, int id, boolean value) throws Exception {
        MiruInvertedIndex<BM> got = getOrAllocate(authz);
        if (value) {
            got.set(id);
        } else {
            got.remove(id);
        }
        cache.increment(authz);
    }

    @Override
    public void close() {
        cache.clear();
    }

    private MiruInvertedIndex<BM> getOrAllocate(String authz) {
        return new MiruInMemoryInvertedIndex<>(bitmaps, store, authz.getBytes(Charsets.UTF_8), -1);
    }

    @Override
    public BM getCompositeAuthz(MiruAuthzExpression authzExpression) throws Exception {
        return cache.getOrCompose(authzExpression, new MiruAuthzUtils.IndexRetriever<BM>() {
            @Override
            public BM getIndex(String authz) throws Exception {
                return getOrAllocate(authz).getIndex().orNull();
            }
        });
    }

    @Override
    public Void bulkExport(MiruTenantId tenantId, final KeyedInvertedIndexStream<BM> callback) throws Exception {
        store.streamKeys(new KeyValueStore.KeyStream<byte[]>() {
            @Override
            public boolean stream(byte[] bytes) throws IOException {
                return callback.stream(bytes, new MiruInMemoryInvertedIndex<>(bitmaps, store, bytes, -1));
            }
        });
        return null;
    }

    @Override
    public void bulkImport(MiruTenantId tenantId, BulkExport<Void, KeyedInvertedIndexStream<BM>> export) throws Exception {
        export.bulkExport(tenantId, new KeyedInvertedIndexStream<BM>() {
            @Override
            public boolean stream(byte[] key, final MiruInvertedIndex<BM> invertedIndex) throws IOException {
                store.execute(key, true, new KeyValueTransaction<ReadWrite<BM>, Void>() {
                    @Override
                    public Void commit(KeyValueContext<ReadWrite<BM>> keyValueContext) throws IOException {
                        try {
                            keyValueContext.set(new ReadWrite<>(invertedIndex.getIndex().get(), invertedIndex.lastId()));
                            return null;
                        } catch (Exception e) {
                            throw new IOException("Failed to stream import", e);
                        }
                    }
                });
                return true;
            }
        });
    }
}
