package com.jivesoftware.os.miru.service.index.memory;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.jivesoftware.os.filer.map.store.VariableKeySizeBytesObjectMapStore;
import com.jivesoftware.os.filer.map.store.api.KeyValueStore;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.api.base.MiruTermId;
import com.jivesoftware.os.miru.plugin.bitmap.MiruBitmaps;
import com.jivesoftware.os.miru.plugin.index.MiruFieldIndex;
import com.jivesoftware.os.miru.plugin.index.MiruInvertedIndex;
import com.jivesoftware.os.miru.service.index.BulkEntry;
import com.jivesoftware.os.miru.service.index.BulkExport;
import com.jivesoftware.os.miru.service.index.BulkImport;
import java.util.Iterator;
import java.util.List;

/**
 * @author jonathan
 */
public class MiruHybridFieldIndex<BM> implements MiruFieldIndex<BM>, BulkImport<Iterator<Iterator<BulkEntry<byte[], MiruInvertedIndex<BM>>>>>,
    BulkExport<Iterator<Iterator<BulkEntry<byte[], MiruInvertedIndex<BM>>>>> {

    private final MiruBitmaps<BM> bitmaps;
    private final VariableKeySizeBytesObjectMapStore<byte[], MiruInvertedIndex<BM>>[] indexes;

    public MiruHybridFieldIndex(MiruBitmaps<BM> bitmaps, VariableKeySizeBytesObjectMapStore<byte[], MiruInvertedIndex<BM>>[] indexes) {
        this.bitmaps = bitmaps;
        this.indexes = indexes;
    }

    @Override
    public long sizeInMemory() throws Exception {
        long sizeInBytes = 0;
        for (VariableKeySizeBytesObjectMapStore<byte[], MiruInvertedIndex<BM>> index : indexes) {
            synchronized (index) {
                for (KeyValueStore.Entry<byte[], MiruInvertedIndex<BM>> entry : index) {
                    sizeInBytes += bitmaps.sizeInBytes(entry.getValue().getIndex());
                }
            }
        }
        return sizeInBytes;
    }

    @Override
    public long sizeOnDisk() throws Exception {
        return 0;
    }

    @Override
    public void index(int fieldId, MiruTermId termId, int... ids) throws Exception {
        getOrAllocate(fieldId, termId).append(ids);
    }

    @Override
    public void remove(int fieldId, MiruTermId termId, int id) throws Exception {
        Optional<MiruInvertedIndex<BM>> got = get(fieldId, termId);
        if (got.isPresent()) {
            got.get().remove(id);
        }
    }

    @Override
    public Iterator<MiruTermId> getTermIdsForField(int fieldId) throws Exception {
        return Iterators.transform(indexes[fieldId].keysIterator(), new Function<byte[], MiruTermId>() {
            @Override
            public MiruTermId apply(byte[] input) {
                return new MiruTermId(input);
            }
        });
    }

    @Override
    public Optional<MiruInvertedIndex<BM>> get(int fieldId, MiruTermId termId) throws Exception {
        return Optional.fromNullable(indexes[fieldId].getUnsafe(termId.getBytes()));
    }

    @Override
    public Optional<MiruInvertedIndex<BM>> get(int fieldId, MiruTermId termId, int considerIfIndexIdGreaterThanN) throws Exception {
        MiruInvertedIndex<BM> unsafe = indexes[fieldId].getUnsafe(termId.getBytes());
        if (unsafe != null) {
            if (unsafe.lastId() <= considerIfIndexIdGreaterThanN) {
                unsafe = null;
            }
        }
        return Optional.fromNullable(unsafe);
    }

    @Override
    public MiruInvertedIndex<BM> getOrCreateInvertedIndex(int fieldId, MiruTermId term) throws Exception {
        return getOrAllocate(fieldId, term);
    }

    private MiruInvertedIndex<BM> getOrAllocate(int fieldId, MiruTermId termId) throws Exception {
        byte[] key = termId.getBytes();
        VariableKeySizeBytesObjectMapStore<byte[], MiruInvertedIndex<BM>> index = indexes[fieldId];
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
    public Iterator<Iterator<BulkEntry<byte[], MiruInvertedIndex<BM>>>> bulkExport(MiruTenantId tenantId) throws Exception {
        List<Iterator<BulkEntry<byte[], MiruInvertedIndex<BM>>>> iterators = Lists.newArrayListWithCapacity(indexes.length);
        for (VariableKeySizeBytesObjectMapStore<byte[], MiruInvertedIndex<BM>> index : indexes) {
            iterators.add(Iterators.transform(index.iterator(),
                new Function<KeyValueStore.Entry<byte[], MiruInvertedIndex<BM>>, BulkEntry<byte[], MiruInvertedIndex<BM>>>() {
                    @Override
                    public BulkEntry<byte[], MiruInvertedIndex<BM>> apply(KeyValueStore.Entry<byte[], MiruInvertedIndex<BM>> input) {
                        return new BulkEntry<>(input.getKey(), input.getValue());
                    }
                }));
        }
        return iterators.iterator();
    }

    @Override
    public void bulkImport(MiruTenantId tenantId, BulkExport<Iterator<Iterator<BulkEntry<byte[], MiruInvertedIndex<BM>>>>> importItems) throws Exception {
        int fieldId = 0;
        Iterator<Iterator<BulkEntry<byte[], MiruInvertedIndex<BM>>>> fieldIterator = importItems.bulkExport(tenantId);
        while (fieldIterator.hasNext()) {
            Iterator<BulkEntry<byte[], MiruInvertedIndex<BM>>> iter = fieldIterator.next();
            VariableKeySizeBytesObjectMapStore<byte[], MiruInvertedIndex<BM>> index = indexes[fieldId];
            synchronized (index) {
                while (iter.hasNext()) {
                    BulkEntry<byte[], MiruInvertedIndex<BM>> entry = iter.next();
                    index.add(entry.key, entry.value);
                }
            }
            fieldId++;
        }
    }

    public void copyTo(MiruHybridFieldIndex<BM> to) throws Exception {
        for (int i = 0; i < indexes.length; i++) {
            indexes[i].copyTo(to.indexes[i]);
        }
    }
}
