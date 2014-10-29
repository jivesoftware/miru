package com.jivesoftware.os.miru.service.index.memory;

import com.google.common.base.Optional;
import com.jivesoftware.os.miru.api.MiruPartitionState;
import com.jivesoftware.os.miru.api.activity.schema.MiruFieldDefinition;
import com.jivesoftware.os.miru.api.base.MiruTermId;
import com.jivesoftware.os.miru.plugin.index.MiruField;
import com.jivesoftware.os.miru.plugin.index.MiruInvertedIndex;

/**
 * Term dictionary is in memory. Supports index(). Next term id is held in memory.
 */
public class MiruInMemoryField<BM> implements MiruField<BM> {

    private final MiruFieldDefinition fieldDefinition;
    private final MiruInMemoryIndex<BM> index;

    public MiruInMemoryField(MiruFieldDefinition fieldDefinition, MiruInMemoryIndex<BM> index) {
        this.fieldDefinition = fieldDefinition;
        this.index = index;
    }

    @Override
    public MiruFieldDefinition getFieldDefinition() {
        return fieldDefinition;
    }

    @Override
    public void notifyStateChange(MiruPartitionState state) {
        // do nothing
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
    public void index(MiruTermId term, int... ids) throws Exception {
        index.index(fieldDefinition.fieldId, term, ids);
    }

    @Override
    public void remove(MiruTermId term, int id) throws Exception {
        index.remove(fieldDefinition.fieldId, term, id);
    }

    @Override
    public MiruInvertedIndex<BM> getOrCreateInvertedIndex(MiruTermId term) throws Exception {
        Optional<MiruInvertedIndex<BM>> invertedIndex = getInvertedIndex(term);
        if (invertedIndex.isPresent()) {
            return invertedIndex.get();
        }
        return index.allocate(fieldDefinition.fieldId, term);
    }

    @Override
    public Optional<MiruInvertedIndex<BM>> getInvertedIndex(MiruTermId term, int considerIfIndexIdGreaterThanN) throws Exception {
        return index.get(fieldDefinition.fieldId, term, considerIfIndexIdGreaterThanN);
    }

    @Override
    public Optional<MiruInvertedIndex<BM>> getInvertedIndex(MiruTermId term) throws Exception {
        return index.get(fieldDefinition.fieldId, term);
    }

    public MiruInMemoryIndex<BM> getIndex() {
        return index;
    }
}
