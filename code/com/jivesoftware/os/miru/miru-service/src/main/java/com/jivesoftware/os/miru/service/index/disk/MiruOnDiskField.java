package com.jivesoftware.os.miru.service.index.disk;

import com.google.common.base.Optional;
import com.jivesoftware.os.miru.api.MiruPartitionState;
import com.jivesoftware.os.miru.api.activity.schema.MiruFieldDefinition;
import com.jivesoftware.os.miru.api.base.MiruTermId;
import com.jivesoftware.os.miru.plugin.index.MiruField;
import com.jivesoftware.os.miru.plugin.index.MiruInvertedIndex;

/**
 * Persistent impl. Term dictionary is mem-mapped. Does not support index(), so no next term id.
 */
public class MiruOnDiskField<BM> implements MiruField<BM> {

    private final MiruFieldDefinition fieldDefinition;
    private final MiruOnDiskIndex<BM> index;

    public MiruOnDiskField(MiruFieldDefinition fieldDefinition, MiruOnDiskIndex<BM> index) throws Exception {
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
        throw new UnsupportedOperationException("On disk index is read only");
    }

    @Override
    public void remove(MiruTermId term, int id) throws Exception {
        Optional<MiruInvertedIndex<BM>> index = getInvertedIndex(term);
        if (index.isPresent()) {
            index.get().remove(id);
        }
    }

    @Override
    public MiruInvertedIndex<BM> getOrCreateInvertedIndex(MiruTermId term) throws Exception {
        throw new UnsupportedOperationException("On disk index is read only");
    }

    @Override
    public Optional<MiruInvertedIndex<BM>> getInvertedIndex(MiruTermId term, int considerIfIndexIdGreaterThanN) throws Exception {
        return index.get(fieldDefinition.fieldId, term, considerIfIndexIdGreaterThanN);
    }

    @Override
    public Optional<MiruInvertedIndex<BM>> getInvertedIndex(MiruTermId term) throws Exception {
        return index.get(fieldDefinition.fieldId, term);
    }

    public MiruOnDiskIndex<BM> getIndex() {
        return index;
    }
}
