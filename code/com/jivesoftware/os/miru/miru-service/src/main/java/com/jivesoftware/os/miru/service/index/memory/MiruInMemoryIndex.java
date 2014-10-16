package com.jivesoftware.os.miru.service.index.memory;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.collect.Iterators;
import com.jivesoftware.os.filer.io.ByteBufferFactory;
import com.jivesoftware.os.filer.io.FilerIO;
import com.jivesoftware.os.filer.io.HeapByteBufferFactory;
import com.jivesoftware.os.filer.map.store.BytesObjectMapStore;
import com.jivesoftware.os.filer.map.store.api.KeyValueStore;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.plugin.bitmap.MiruBitmaps;
import com.jivesoftware.os.miru.plugin.index.MiruIndex;
import com.jivesoftware.os.miru.plugin.index.MiruInvertedIndex;
import com.jivesoftware.os.miru.service.index.BulkEntry;
import com.jivesoftware.os.miru.service.index.BulkExport;
import com.jivesoftware.os.miru.service.index.BulkImport;
import com.jivesoftware.os.miru.service.index.IndexKeyFunction;
import java.util.Iterator;

/**
 * @author jonathan
 */
public class MiruInMemoryIndex<BM> implements MiruIndex<BM>, BulkImport<Iterator<BulkEntry<Long, MiruInvertedIndex<BM>>>>,
        BulkExport<Iterator<BulkEntry<Long, MiruInvertedIndex<BM>>>> {

    private final MiruBitmaps<BM> bitmaps;
    private final BytesObjectMapStore<Long, MiruInvertedIndex<BM>> index;
    private final IndexKeyFunction indexKeyFunction = new IndexKeyFunction();

    public MiruInMemoryIndex(MiruBitmaps<BM> bitmaps, ByteBufferFactory byteBufferFactory) {
        this.bitmaps = bitmaps;
        this.index = new BytesObjectMapStore<Long, MiruInvertedIndex<BM>>(8, 10, null, byteBufferFactory) {
            @Override
            public byte[] keyBytes(Long key) {
                return FilerIO.longBytes(key);
            }

            @Override
            public Long bytesKey(byte[] bytes, int offset) {
                return FilerIO.bytesLong(bytes, offset);
            }
        };
    }

    @Override
    public long sizeInMemory() throws Exception {
        long sizeInBytes = 0;
        synchronized (index) {
            for (KeyValueStore.Entry<Long, MiruInvertedIndex<BM>> entry : index) {
                sizeInBytes += bitmaps.sizeInBytes(entry.getValue().getIndex());
            }
        }
        return sizeInBytes;
    }

    @Override
    public long sizeOnDisk() throws Exception {
        return 0;
    }

    @Override
    public MiruInvertedIndex<BM> allocate(int fieldId, int termId) throws Exception {
        return getOrAllocate(fieldId, termId);
    }

    @Override
    public void index(int fieldId, int termId, int... ids) throws Exception {
        getOrAllocate(fieldId, termId).append(ids);
    }

    @Override
    public void remove(int fieldId, int termId, int id) throws Exception {
        Optional<MiruInvertedIndex<BM>> got = get(fieldId, termId);
        if (got.isPresent()) {
            got.get().remove(id);
        }
    }

    @Override
    public Optional<MiruInvertedIndex<BM>> get(int fieldId, int termId) throws Exception {
        long key = indexKeyFunction.getKey(fieldId, termId);
        return Optional.fromNullable(index.getUnsafe(key));
    }

    private MiruInvertedIndex<BM> getOrAllocate(int fieldId, int termId) throws Exception {
        long key = indexKeyFunction.getKey(fieldId, termId);
        MiruInvertedIndex<BM> got = index.getUnsafe(key);
        if (got == null) {
            synchronized (index) {
                got = index.get(key);
                if (got == null) {
                    got = new MiruInMemoryInvertedIndex<>(bitmaps);
                    index.add(key, got);
                }
            }
        }
        return got;
    }

    @Override
    public Iterator<BulkEntry<Long, MiruInvertedIndex<BM>>> bulkExport(MiruTenantId tenantId) throws Exception {
        return Iterators.transform(index.iterator(), new Function<KeyValueStore.Entry<Long, MiruInvertedIndex<BM>>, BulkEntry<Long, MiruInvertedIndex<BM>>>() {
            @Override
            public BulkEntry<Long, MiruInvertedIndex<BM>> apply(KeyValueStore.Entry<Long, MiruInvertedIndex<BM>> input) {
                return new BulkEntry<>(input.getKey(), input.getValue());
            }
        });
    }

    @Override
    public void bulkImport(MiruTenantId tenantId, BulkExport<Iterator<BulkEntry<Long, MiruInvertedIndex<BM>>>> importItems) throws Exception {
        synchronized (index) {
            Iterator<BulkEntry<Long, MiruInvertedIndex<BM>>> iter = importItems.bulkExport(tenantId);
            while (iter.hasNext()) {
                BulkEntry<Long, MiruInvertedIndex<BM>> entry = iter.next();
                index.add(entry.key, entry.value);
            }
        }
    }
}
