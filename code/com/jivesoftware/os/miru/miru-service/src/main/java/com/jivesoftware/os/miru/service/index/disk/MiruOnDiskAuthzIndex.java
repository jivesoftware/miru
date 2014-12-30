package com.jivesoftware.os.miru.service.index.disk;

import com.google.common.base.Optional;
import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import com.google.common.io.BaseEncoding;
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
import java.util.List;

/** @author jonathan */
public class MiruOnDiskAuthzIndex<BM> implements MiruAuthzIndex<BM>,
    BulkImport<Void, KeyedInvertedIndexStream<BM>> {

    static final BaseEncoding coder = BaseEncoding.base32().lowerCase().omitPadding();
    static final Splitter splitter = Splitter.on('.');

    private final MiruBitmaps<BM> bitmaps;
    private final KeyedFilerStore keyedStore;
    private final MiruAuthzCache<BM> cache;
    private final StripingLocksProvider<String> stripingLocksProvider;

    public MiruOnDiskAuthzIndex(MiruBitmaps<BM> bitmaps,
        KeyedFilerStore keyedStore,
        MiruAuthzCache<BM> cache,
        StripingLocksProvider<String> stripingLocksProvider)
        throws Exception {

        this.bitmaps = bitmaps;
        this.keyedStore = keyedStore;
        this.cache = cache;
        this.stripingLocksProvider = stripingLocksProvider;

        /*
        long newFilerInitialCapacity = 512;
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
                stripingLocksProvider,
                4)); //TODO expose num partitions to config
        }
        this.keyedStore = keyedStoreBuilder.build();
        */
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
    public void index(String authz, int id) {
        throw new UnsupportedOperationException("On disk authz indexes are readOnly");
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
        return new MiruOnDiskInvertedIndex<>(bitmaps, keyedStore, key(authz), -1, -1, stripingLocksProvider.lock(authz));
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
                return get(authz).getIndex().orNull();
            }
        });
    }

    @Override
    public void bulkImport(final MiruTenantId tenantId, BulkExport<Void, KeyedInvertedIndexStream<BM>> export) throws Exception {
        export.bulkExport(tenantId, new KeyedInvertedIndexStream<BM>() {
            @Override
            public boolean stream(byte[] key, MiruInvertedIndex<BM> importIndex) throws IOException {
                try {
                    Optional<BM> index = importIndex.getIndex();
                    if (index.isPresent()) {
                        long importFilerCapacity = MiruOnDiskInvertedIndex.serializedSizeInBytes(bitmaps, index.get());
                        MiruOnDiskInvertedIndex<BM> invertedIndex = new MiruOnDiskInvertedIndex<>(
                            bitmaps, keyedStore, key, -1, importFilerCapacity, new Object());
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
