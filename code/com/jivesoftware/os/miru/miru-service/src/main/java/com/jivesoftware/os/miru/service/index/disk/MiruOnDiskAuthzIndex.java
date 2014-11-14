package com.jivesoftware.os.miru.service.index.disk;

import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import com.google.common.io.BaseEncoding;
import com.jivesoftware.os.filer.chunk.store.MultiChunkStore;
import com.jivesoftware.os.filer.io.StripingLocksProvider;
import com.jivesoftware.os.filer.keyed.store.PartitionedMapChunkBackedKeyedStore;
import com.jivesoftware.os.filer.keyed.store.SwappableFiler;
import com.jivesoftware.os.filer.keyed.store.VariableKeySizeMapChunkBackedKeyedStore;
import com.jivesoftware.os.filer.map.store.FileBackedMapChunkFactory;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.api.query.filter.MiruAuthzExpression;
import com.jivesoftware.os.miru.plugin.bitmap.MiruBitmaps;
import com.jivesoftware.os.miru.plugin.index.MiruAuthzIndex;
import com.jivesoftware.os.miru.plugin.index.MiruInvertedIndex;
import com.jivesoftware.os.miru.service.index.BitmapAndLastId;
import com.jivesoftware.os.miru.service.index.BulkExport;
import com.jivesoftware.os.miru.service.index.BulkImport;
import com.jivesoftware.os.miru.service.index.auth.MiruAuthzCache;
import com.jivesoftware.os.miru.service.index.auth.MiruAuthzUtils;
import java.io.File;
import java.util.List;
import java.util.Map;

/** @author jonathan */
public class MiruOnDiskAuthzIndex<BM> implements MiruAuthzIndex<BM>, BulkImport<Map<String, MiruInvertedIndex<BM>>> {

    static final BaseEncoding coder = BaseEncoding.base32().lowerCase().omitPadding();
    static final Splitter splitter = Splitter.on('.');

    private final MiruBitmaps<BM> bitmaps;
    private final VariableKeySizeMapChunkBackedKeyedStore keyedStore;
    private final MiruAuthzCache<BM> cache;
    private final StripingLocksProvider<String> stripingLocksProvider = new StripingLocksProvider<>(32);
    private final long newFilerInitialCapacity = 512;

    public MiruOnDiskAuthzIndex(MiruBitmaps<BM> bitmaps,
        String[] baseMapDirectories,
        String[] baseSwapDirectories,
        MultiChunkStore chunkStore,
        MiruAuthzCache<BM> cache)
        throws Exception {

        this.bitmaps = bitmaps;

        int[] keySizeThresholds = { 4, 16, 64, 256, 1_024 };
        //TODO actual capacity? should this be shared with a key prefix?
        //TODO expose to config
        VariableKeySizeMapChunkBackedKeyedStore.Builder keyedStoreBuilder = new VariableKeySizeMapChunkBackedKeyedStore.Builder();
        for (int keySize : keySizeThresholds) {
            String[] mapDirectories = new String[baseMapDirectories.length];
            for (int i = 0; i < mapDirectories.length; i++) {
                mapDirectories[i] = new File(baseMapDirectories[i], String.valueOf(keySize)).getAbsolutePath();
            }
            String[] swapDirectories = new String[baseSwapDirectories.length];
            for (int i = 0; i < mapDirectories.length; i++) {
                swapDirectories[i] = new File(baseSwapDirectories[i], String.valueOf(keySize)).getAbsolutePath();
            }
            keyedStoreBuilder.add(keySize, new PartitionedMapChunkBackedKeyedStore(
                new FileBackedMapChunkFactory(keySize, false, 8, false, 100, mapDirectories),
                new FileBackedMapChunkFactory(keySize, false, 8, false, 100, swapDirectories),
                chunkStore,
                4)); //TODO expose num partitions to config
        }

        this.keyedStore = keyedStoreBuilder.build();
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
        SwappableFiler filer = keyedStore.get(key(authz), -1);
        if (filer == null) {
            return null;
        }
        return new MiruOnDiskInvertedIndex<>(bitmaps, filer, stripingLocksProvider.lock(authz));
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
            final MiruInvertedIndex<BM> fromMiruInvertedIndex = entry.getValue();

            SwappableFiler filer = keyedStore.get(key(entry.getKey()), newFilerInitialCapacity);
            MiruOnDiskInvertedIndex<BM> toMiruInvertedIndex = new MiruOnDiskInvertedIndex<>(bitmaps, filer, stripingLocksProvider.lock(entry.getKey()));
            toMiruInvertedIndex.bulkImport(tenantId, new BulkExport<BitmapAndLastId<BM>>() {
                @Override
                public BitmapAndLastId<BM> bulkExport(MiruTenantId tenantId) throws Exception {
                    return new BitmapAndLastId<>(fromMiruInvertedIndex.getIndex(), fromMiruInvertedIndex.lastId());
                }
            });
        }
    }
}
