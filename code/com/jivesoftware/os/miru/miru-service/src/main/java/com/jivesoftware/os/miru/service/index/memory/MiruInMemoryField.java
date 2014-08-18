package com.jivesoftware.os.miru.service.index.memory;

import com.google.common.base.Optional;
import com.google.common.collect.Maps;
import com.jivesoftware.os.miru.api.base.MiruTermId;
import com.jivesoftware.os.miru.service.index.BulkExport;
import com.jivesoftware.os.miru.service.index.MiruField;
import com.jivesoftware.os.miru.service.index.MiruFieldIndexKey;
import com.jivesoftware.os.miru.service.index.MiruInvertedIndex;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Term dictionary is in memory. Supports index(). Next term id is held in memory.
 */
public class MiruInMemoryField implements MiruField, BulkExport<Map<MiruTermId, MiruFieldIndexKey>> {

    private final int fieldId;
    private final ConcurrentMap<MiruTermId, MiruFieldIndexKey> termToIndex;
    private final MiruInMemoryIndex index;
    private final AtomicInteger nextTermId;

    public MiruInMemoryField(int fieldId, Map<MiruTermId, MiruFieldIndexKey> termToIndex, MiruInMemoryIndex index) {
        this.fieldId = fieldId;
        this.termToIndex = new ConcurrentHashMap<>(termToIndex);
        this.index = index;
        this.nextTermId = new AtomicInteger();
    }

    @Override
    public long sizeInMemory() throws Exception {
        long sizeInBytes = termToIndex.size() * 16; // 2 refs
        for (Map.Entry<MiruTermId, MiruFieldIndexKey> entry : termToIndex.entrySet()) {
            sizeInBytes += entry.getKey().getBytes().length + entry.getValue().sizeInBytes();
        }
        return sizeInBytes;
    }

    @Override
    public long sizeOnDisk() throws Exception {
        return 0;
    }

    @Override
    public void index(MiruTermId term, int id) throws Exception {
        MiruFieldIndexKey indexKey = getOrCreateTermId(term);
        index.index(fieldId, indexKey.getId(), id);
        indexKey.retain(id);
    }

    @Override
    public void remove(MiruTermId term, int id) throws Exception {
        MiruFieldIndexKey indexKey = getOrCreateTermId(term);
        index.remove(fieldId, indexKey.getId(), id);
    }

    @Override
    public Optional<MiruInvertedIndex> getOrCreateInvertedIndex(MiruTermId term) throws Exception {
        MiruFieldIndexKey indexKey = getOrCreateTermId(term);
        Optional<MiruInvertedIndex> invertedIndex = getInvertedIndex(indexKey);
        if (invertedIndex.isPresent()) {
            return invertedIndex;
        }
        index.allocate(fieldId, indexKey.getId());
        return getInvertedIndex(indexKey);
    }

    @Override
    public Optional<MiruInvertedIndex> getInvertedIndex(MiruTermId term, int considerIfIndexIdGreaterThanN) throws Exception {
        Optional<MiruFieldIndexKey> indexKey = getTermId(term);
        if (indexKey.isPresent() && indexKey.get().getMaxId() > considerIfIndexIdGreaterThanN) {
            return getInvertedIndex(term);
        }
        return Optional.absent();
    }

    @Override
    public Optional<MiruInvertedIndex> getInvertedIndex(MiruTermId term) throws Exception {
        Optional<MiruFieldIndexKey> indexKey = getTermId(term);
        if (indexKey.isPresent()) {
            return getInvertedIndex(indexKey.get());
        }
        return Optional.absent();
    }

    public MiruInMemoryIndex getIndex() {
        return index;
    }

    private Optional<MiruInvertedIndex> getInvertedIndex(MiruFieldIndexKey indexKey) throws Exception {
        if (indexKey != null) {
            return index.get(fieldId, indexKey.getId());
        }
        return Optional.absent();
    }

    private Optional<MiruFieldIndexKey> getTermId(MiruTermId term) {
        MiruFieldIndexKey id = termToIndex.get(term);
        return Optional.fromNullable(id);
    }

    private MiruFieldIndexKey getOrCreateTermId(MiruTermId term) {
        MiruFieldIndexKey id = termToIndex.get(term);
        if (id == null) {
            termToIndex.putIfAbsent(term, new MiruFieldIndexKey(nextTermId.getAndIncrement()));
            id = termToIndex.get(term);
        }
        return id;
    }

    @Override
    public Map<MiruTermId, MiruFieldIndexKey> bulkExport() throws Exception {
        return Maps.newHashMap(termToIndex);
    }
}
