package com.jivesoftware.os.miru.service.index.disk;

import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import com.google.common.io.BaseEncoding;
import com.googlecode.javaewah.EWAHCompressedBitmap;
import com.jivesoftware.os.miru.api.query.filter.MiruAuthzExpression;
import com.jivesoftware.os.miru.service.index.BulkExport;
import com.jivesoftware.os.miru.service.index.BulkImport;
import com.jivesoftware.os.miru.service.index.MiruAuthzCache;
import com.jivesoftware.os.miru.service.index.MiruAuthzIndex;
import com.jivesoftware.os.miru.service.index.MiruAuthzUtils;
import com.jivesoftware.os.miru.service.index.MiruInvertedIndex;
import com.jivesoftware.os.jive.utils.chunk.store.ChunkStore;
import com.jivesoftware.os.jive.utils.io.Filer;
import com.jivesoftware.os.jive.utils.io.FilerIO;
import com.jivesoftware.os.jive.utils.keyed.store.SwappableFiler;
import com.jivesoftware.os.jive.utils.keyed.store.VariableKeySizeFileBackedKeyedStore;
import java.io.File;
import java.util.List;
import java.util.Map;

/** @author jonathan */
public class MiruOnDiskAuthzIndex implements MiruAuthzIndex, BulkImport<Map<String, MiruInvertedIndex>> {

    static final BaseEncoding coder = BaseEncoding.base32().lowerCase().omitPadding();
    static final Splitter splitter = Splitter.on('.');

    private final VariableKeySizeFileBackedKeyedStore keyedStore;
    private final MiruAuthzCache cache;

    public MiruOnDiskAuthzIndex(File mapDirectory, File swapDirectory, ChunkStore chunkStore, MiruAuthzCache cache) throws Exception {
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

    private MiruInvertedIndex get(String authz) throws Exception {
        SwappableFiler filer = keyedStore.get(key(authz), false);
        if (filer == null) {
            return null;
        }
        return new MiruOnDiskInvertedIndex(filer);
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
    public EWAHCompressedBitmap getCompositeAuthz(MiruAuthzExpression authzExpression) throws Exception {
        return cache.getOrCompose(authzExpression, new MiruAuthzUtils.IndexRetriever() {
            @Override
            public EWAHCompressedBitmap getIndex(String authz) throws Exception {
                MiruInvertedIndex index = get(authz);
                return index != null ? index.getIndex() : null;
            }
        });
    }

    @Override
    public void bulkImport(BulkExport<Map<String, MiruInvertedIndex>> importItems) throws Exception {
        Map<String, MiruInvertedIndex> authzMap = importItems.bulkExport();
        for (Map.Entry<String, MiruInvertedIndex> entry : authzMap.entrySet()) {
            MiruInvertedIndex miruInvertedIndex = entry.getValue();

            Filer filer = keyedStore.get(key(entry.getKey()));

            miruInvertedIndex.getIndex().serialize(FilerIO.asDataOutput(filer));
        }
    }
}
