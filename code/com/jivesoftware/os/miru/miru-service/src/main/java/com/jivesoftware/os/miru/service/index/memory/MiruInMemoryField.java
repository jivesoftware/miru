package com.jivesoftware.os.miru.service.index.memory;

import com.google.common.base.Optional;
import com.google.common.collect.Iterators;
import com.jivesoftware.os.filer.map.store.VariableKeySizeBytesObjectMapStore;
import com.jivesoftware.os.filer.map.store.api.KeyValueStore;
import com.jivesoftware.os.filer.map.store.api.KeyValueStoreException;
import com.jivesoftware.os.miru.api.activity.schema.MiruFieldDefinition;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.api.base.MiruTermId;
import com.jivesoftware.os.miru.plugin.index.MiruField;
import com.jivesoftware.os.miru.plugin.index.MiruInvertedIndex;
import com.jivesoftware.os.miru.service.index.BulkEntry;
import com.jivesoftware.os.miru.service.index.BulkExport;
import com.jivesoftware.os.miru.service.index.MiruFieldIndexKey;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Term dictionary is in memory. Supports index(). Next term id is held in memory.
 */
public class MiruInMemoryField<BM> implements MiruField<BM>, BulkExport<Iterator<BulkEntry<MiruTermId, MiruFieldIndexKey>>> {

    private final MiruFieldDefinition fieldDefinition;
    private final VariableKeySizeBytesObjectMapStore<MiruTermId, MiruFieldIndexKey> termToIndex;
    private final MiruInMemoryIndex<BM> index;
    private final AtomicInteger nextTermId;

    public MiruInMemoryField(MiruFieldDefinition fieldDefinition, MiruInMemoryIndex<BM> index) {
        this.fieldDefinition = fieldDefinition;
        this.termToIndex = new VariableKeySizeBytesObjectMapStore<MiruTermId, MiruFieldIndexKey>(
            new int[] { 2, 4, 8, 16, 32, 64, 128, 256, 512, 1024 }, 10, null) {

            @Override
            protected int keyLength(MiruTermId key) {
                return key.getBytes().length;
            }

            @Override
            public byte[] keyBytes(MiruTermId key) {
                return key.getBytes();
            }

            @Override
            public MiruTermId bytesKey(byte[] bytes, int offset) {
                return new MiruTermId(bytes);
            }
        };
        this.index = index;
        this.nextTermId = new AtomicInteger();
    }

    @Override
    public MiruFieldDefinition getFieldDefinition() {
        return fieldDefinition;
    }

    @Override
    public long sizeInMemory() throws Exception {
        long sizeInBytes = termToIndex.estimateSizeInBytes() * 16; // 2 refs
        for (KeyValueStore.Entry<MiruTermId, MiruFieldIndexKey> entry : termToIndex) {
            sizeInBytes += entry.getKey().getBytes().length + entry.getValue().sizeInBytes();
        }
        return sizeInBytes;
    }

    @Override
    public long sizeOnDisk() throws Exception {
        return 0;
    }

    @Override
    public void index(MiruTermId term, int... ids) throws Exception {
        MiruFieldIndexKey indexKey = getOrCreateTermId(term);
        index.index(fieldDefinition.fieldId, indexKey.getId(), ids);
        indexKey.retain(ids[ids.length - 1]);
    }

    @Override
    public void remove(MiruTermId term, int id) throws Exception {
        MiruFieldIndexKey indexKey = getOrCreateTermId(term);
        index.remove(fieldDefinition.fieldId, indexKey.getId(), id);
    }

    @Override
    public MiruInvertedIndex<BM> getOrCreateInvertedIndex(MiruTermId term) throws Exception {
        MiruFieldIndexKey indexKey = getOrCreateTermId(term);
        Optional<MiruInvertedIndex<BM>> invertedIndex = getInvertedIndex(indexKey);
        if (invertedIndex.isPresent()) {
            return invertedIndex.get();
        }
        return index.allocate(fieldDefinition.fieldId, indexKey.getId());
    }

    @Override
    public Optional<MiruInvertedIndex<BM>> getInvertedIndex(MiruTermId term, int considerIfIndexIdGreaterThanN) throws Exception {
        Optional<MiruFieldIndexKey> indexKey = getTermId(term);
        if (indexKey.isPresent() && indexKey.get().getMaxId() > considerIfIndexIdGreaterThanN) {
            return getInvertedIndex(term);
        }
        return Optional.absent();
    }

    @Override
    public Optional<MiruInvertedIndex<BM>> getInvertedIndex(MiruTermId term) throws Exception {
        Optional<MiruFieldIndexKey> indexKey = getTermId(term);
        if (indexKey.isPresent()) {
            return getInvertedIndex(indexKey.get());
        }
        return Optional.absent();
    }

    public MiruInMemoryIndex<BM> getIndex() {
        return index;
    }

    private Optional<MiruInvertedIndex<BM>> getInvertedIndex(MiruFieldIndexKey indexKey) throws Exception {
        if (indexKey != null) {
            return index.get(fieldDefinition.fieldId, indexKey.getId());
        }
        return Optional.absent();
    }

    private Optional<MiruFieldIndexKey> getTermId(MiruTermId term) throws KeyValueStoreException {
        MiruFieldIndexKey id = termToIndex.getUnsafe(term);
        return Optional.fromNullable(id);
    }

    private MiruFieldIndexKey getOrCreateTermId(MiruTermId term) throws KeyValueStoreException {
        MiruFieldIndexKey id = termToIndex.getUnsafe(term);
        if (id == null) {
            synchronized (termToIndex) { // yes its double check locking and we know it! Its because we cannot nextTermId.getAndIncrement() prematurely
                id = termToIndex.get(term);
                if (id == null) {
                    termToIndex.add(term, new MiruFieldIndexKey(nextTermId.getAndIncrement()));
                    id = termToIndex.get(term);
                }
            }
        }
        return id;
    }

    @Override
    public Iterator<BulkEntry<MiruTermId, MiruFieldIndexKey>> bulkExport(MiruTenantId tenantId) throws Exception {
        return Iterators.transform(termToIndex.iterator(), BulkEntry.<MiruTermId, MiruFieldIndexKey>fromKeyValueStoreEntry());
    }
}
