package com.jivesoftware.os.miru.service.index.disk;

import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import com.google.common.io.BaseEncoding;
import com.jivesoftware.os.jive.utils.chunk.store.MultiChunkStore;
import com.jivesoftware.os.jive.utils.io.Filer;
import com.jivesoftware.os.jive.utils.io.FilerIO;
import com.jivesoftware.os.jive.utils.keyed.store.SwappableFiler;
import com.jivesoftware.os.jive.utils.keyed.store.VariableKeySizeFileBackedKeyedStore;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.api.query.filter.MiruAuthzExpression;
import com.jivesoftware.os.miru.service.bitmap.MiruBitmaps;
import com.jivesoftware.os.miru.service.index.BulkExport;
import com.jivesoftware.os.miru.service.index.BulkImport;
import com.jivesoftware.os.miru.service.index.MiruInvertedIndex;
import com.jivesoftware.os.miru.service.index.auth.MiruAuthzCache;
import com.jivesoftware.os.miru.service.index.auth.MiruAuthzIndex;
import com.jivesoftware.os.miru.service.index.auth.MiruAuthzUtils;
import java.io.File;
import java.util.List;
import java.util.Map;

/** @author jonathan */
public class MiruOnDiskAuthzIndex<BM> implements MiruAuthzIndex<BM>, BulkImport<Map<String, MiruInvertedIndex<BM>>> {

    static final BaseEncoding coder = BaseEncoding.base32().lowerCase().omitPadding();
    static final Splitter splitter = Splitter.on('.');

    private final MiruBitmaps<BM> bitmaps;
    private final VariableKeySizeFileBackedKeyedStore keyedStore;
    private final MiruAuthzCache<BM> cache;

    public MiruOnDiskAuthzIndex(MiruBitmaps<BM> bitmaps, File mapDirectory, File swapDirectory, MultiChunkStore chunkStore, MiruAuthzCache<BM> cache) throws Exception {
        this.bitmaps = bitmaps;
        //TODO actual capacity? should this be shared with a key prefix?
        this.keyedStore = new VariableKeySizeFileBackedKeyedStore(mapDirectory, swapDirectory, chunkStore, new int[] { 4, 16, 64, 256, 1024 }, 100, 512);
        this.cache = cache;
    }

    @Override
    public long sizeInMemory() throws Exception {
        return cache.sizeInBytes();
    }

    @Override
    public long sizeOnDisk() throws Exception {
        return keyedStore.sizeInBytes();
    }

    @Override
    public void index(String authz, int id) {
        throw new UnsupportedOperationException("On disk indexes are readOnly");
    }

    @Override
    public void repair(String authz, int id, boolean value) throws Exception {
        MiruInvertedIndex index = get(authz);
        if (index != null) {
            if (value) {
                index.set(id);
            } else {
                index.remove(id);
            }
            cache.increment(authz);
        }
    }

    @Override
    public void close() {
        keyedStore.close();
        cache.clear();
    }

    private MiruInvertedIndex<BM> get(String authz) throws Exception {
        SwappableFiler filer = keyedStore.get(key(authz), false);
        if (filer == null) {
            return null;
        }
        return new MiruOnDiskInvertedIndex<>(bitmaps, filer);
    }

    private static byte[] key(String authz) {
        boolean negated = authz.endsWith("#");
        if (negated) {
            authz = authz.substring(0, authz.length() - 1);
        }
        List<byte[]> bytesList = Lists.newArrayList();
        for (String authzComponent : splitter.split(authz)) {
            byte[] bytes = coder.decode(authzComponent);
            bytesList.add(bytes);
        }
        if (negated) {
            bytesList.add(new byte[] { 0 });
        }
        int length = bytesList.size() * 8;
        byte[] concatenatedAuthzBytes = new byte[length];
        int i = 0;
        for (byte[] bytes : bytesList) {
            System.arraycopy(bytes, 0, concatenatedAuthzBytes, i * 8, bytes.length);
            i++;
        }
        return concatenatedAuthzBytes;
    }

    @Override
    public BM getCompositeAuthz(MiruAuthzExpression authzExpression) throws Exception {
        return cache.getOrCompose(authzExpression, new MiruAuthzUtils.IndexRetriever<BM>() {
            @Override
            public BM getIndex(String authz) throws Exception {
                MiruInvertedIndex<BM> index = get(authz);
                return index != null ? index.getIndex() : null;
            }
        });
    }

    @Override
    public void bulkImport(MiruTenantId tenantId, BulkExport<Map<String, MiruInvertedIndex<BM>>> importItems) throws Exception {
        Map<String, MiruInvertedIndex<BM>> authzMap = importItems.bulkExport(tenantId);
        for (Map.Entry<String, MiruInvertedIndex<BM>> entry : authzMap.entrySet()) {
            MiruInvertedIndex<BM> miruInvertedIndex = entry.getValue();

            Filer filer = keyedStore.get(key(entry.getKey()));

            bitmaps.serialize(miruInvertedIndex.getIndex(), FilerIO.asDataOutput(filer));
        }
    }
}
