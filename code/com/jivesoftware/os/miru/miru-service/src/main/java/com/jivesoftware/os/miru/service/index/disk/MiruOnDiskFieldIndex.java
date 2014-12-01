package com.jivesoftware.os.miru.service.index.disk;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.jivesoftware.os.filer.io.IBA;
import com.jivesoftware.os.filer.io.StripingLocksProvider;
import com.jivesoftware.os.filer.keyed.store.SwappableFiler;
import com.jivesoftware.os.filer.keyed.store.VariableKeySizeMapChunkBackedKeyedStore;
import com.jivesoftware.os.filer.map.store.api.KeyValueStore;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.api.base.MiruTermId;
import com.jivesoftware.os.miru.plugin.bitmap.MiruBitmaps;
import com.jivesoftware.os.miru.plugin.index.MiruFieldIndex;
import com.jivesoftware.os.miru.plugin.index.MiruInvertedIndex;
import com.jivesoftware.os.miru.service.index.BitmapAndLastId;
import com.jivesoftware.os.miru.service.index.BulkEntry;
import com.jivesoftware.os.miru.service.index.BulkExport;
import com.jivesoftware.os.miru.service.index.BulkImport;
import java.util.Iterator;
import java.util.List;

/**
 * @author jonathan
 */
public class MiruOnDiskFieldIndex<BM> implements MiruFieldIndex<BM>, BulkImport<Iterator<Iterator<BulkEntry<byte[], MiruInvertedIndex<BM>>>>>,
    BulkExport<Iterator<Iterator<BulkEntry<byte[], MiruInvertedIndex<BM>>>>> {

    private final MiruBitmaps<BM> bitmaps;
    private final VariableKeySizeMapChunkBackedKeyedStore[] indexes;
    // We could lock on both field + termId for improved hash/striping, but we favor just termId to reduce object creation
    private final StripingLocksProvider<MiruTermId> stripingLocksProvider = new StripingLocksProvider<>(1024); //TODO expose to config
    private final long newFilerInitialCapacity = 512;

    public MiruOnDiskFieldIndex(MiruBitmaps<BM> bitmaps, VariableKeySizeMapChunkBackedKeyedStore[] indexes) throws Exception {
        this.bitmaps = bitmaps;
        this.indexes = indexes;
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
        return Iterators.transform(indexes[fieldId].keysIterator(), new Function<IBA, MiruTermId>() {
            @Override
            public MiruTermId apply(IBA input) {
                return new MiruTermId(input.getBytes());
            }
        });
    }

    @Override
    public Optional<MiruInvertedIndex<BM>> get(int fieldId, MiruTermId termId) throws Exception {
        SwappableFiler filer = indexes[fieldId].get(termId.getBytes(), -1);
        if (filer == null) {
            return Optional.absent();
        }
        return Optional.<MiruInvertedIndex<BM>>of(new MiruOnDiskInvertedIndex<>(bitmaps, filer,
            stripingLocksProvider.lock(termId)));
    }

    @Override
    public Optional<MiruInvertedIndex<BM>> get(int fieldId, MiruTermId termId, int considerIfIndexIdGreaterThanN) throws Exception {
        SwappableFiler filer = indexes[fieldId].get(termId.getBytes(), -1);
        if (filer == null) {
            return Optional.absent();
        }
        MiruOnDiskInvertedIndex<BM> invertedIndex = new MiruOnDiskInvertedIndex<>(bitmaps, filer,
            stripingLocksProvider.lock(termId));
        if (invertedIndex.lastId() <= considerIfIndexIdGreaterThanN) {
            return Optional.absent();
        }
        return Optional.<MiruInvertedIndex<BM>>of(invertedIndex);
    }

    @Override
    public MiruInvertedIndex<BM> getOrCreateInvertedIndex(int fieldId, MiruTermId term) throws Exception {
        return getOrAllocate(fieldId, term);
    }

    private MiruInvertedIndex<BM> getOrAllocate(int fieldId, MiruTermId termId) throws Exception {
        SwappableFiler filer = indexes[fieldId].get(termId.getBytes(), newFilerInitialCapacity);
        return new MiruOnDiskInvertedIndex<>(bitmaps, filer, stripingLocksProvider.lock(termId));
    }

    @Override
    public void bulkImport(MiruTenantId tenantId, BulkExport<Iterator<Iterator<BulkEntry<byte[], MiruInvertedIndex<BM>>>>> importItems) throws Exception {
        Iterator<Iterator<BulkEntry<byte[], MiruInvertedIndex<BM>>>> fieldIterator = importItems.bulkExport(tenantId);
        int fieldId = 0;
        while (fieldIterator.hasNext()) {
            Iterator<BulkEntry<byte[], MiruInvertedIndex<BM>>> iter = fieldIterator.next();
            while (iter.hasNext()) {
                BulkEntry<byte[], MiruInvertedIndex<BM>> entry = iter.next();
                SwappableFiler filer = indexes[fieldId].get(entry.key, newFilerInitialCapacity);

                final BitmapAndLastId<BM> bitmapAndLastId = new BitmapAndLastId<>(entry.value.getIndex(), entry.value.lastId());
                MiruOnDiskInvertedIndex<BM> miruOnDiskInvertedIndex = new MiruOnDiskInvertedIndex<>(bitmaps, filer,
                    stripingLocksProvider.lock(new MiruTermId(entry.key)));
                miruOnDiskInvertedIndex.bulkImport(tenantId, new BulkExport<BitmapAndLastId<BM>>() {
                    @Override
                    public BitmapAndLastId<BM> bulkExport(MiruTenantId tenantId) throws Exception {
                        return bitmapAndLastId;
                    }
                });
            }
            fieldId++;
        }
    }

    @Override
    public Iterator<Iterator<BulkEntry<byte[], MiruInvertedIndex<BM>>>> bulkExport(MiruTenantId tenantId) throws Exception {
        List<Iterator<BulkEntry<byte[], MiruInvertedIndex<BM>>>> iterators = Lists.newArrayListWithCapacity(indexes.length);
        for (VariableKeySizeMapChunkBackedKeyedStore index : indexes) {
            iterators.add(Iterators.transform(index.iterator(),
                new Function<KeyValueStore.Entry<IBA, SwappableFiler>, BulkEntry<byte[], MiruInvertedIndex<BM>>>() {
                    @Override
                    public BulkEntry<byte[], MiruInvertedIndex<BM>> apply(KeyValueStore.Entry<IBA, SwappableFiler> input) {
                        byte[] termBytes = input.getKey().getBytes();
                        return new BulkEntry<byte[], MiruInvertedIndex<BM>>(termBytes,
                            new MiruOnDiskInvertedIndex<>(bitmaps, input.getValue(), stripingLocksProvider.lock(new MiruTermId(termBytes))));
                    }
                }));
        }
        return iterators.iterator();
    }

    public void copyTo(MiruOnDiskFieldIndex<BM> to) throws Exception {
        for (int i = 0; i < indexes.length; i++) {
            indexes[i].copyTo(to.indexes[i]);
        }
    }
}
