package com.jivesoftware.os.miru.plugin.index;

import com.google.common.base.Optional;
import com.jivesoftware.os.miru.api.MiruPartitionState;
import com.jivesoftware.os.miru.api.activity.schema.MiruFieldDefinition;
import com.jivesoftware.os.miru.api.base.MiruTermId;

/** @author jonathan */
public interface MiruField<BM> {

    MiruFieldDefinition getFieldDefinition();

    void notifyStateChange(MiruPartitionState state) throws Exception;

    long sizeInMemory() throws Exception;

    long sizeOnDisk() throws Exception;

    void index(MiruTermId term, int... ids) throws Exception;

    void remove(MiruTermId term, int id) throws Exception;

    Optional<MiruInvertedIndex<BM>> getInvertedIndex(MiruTermId term, int considerIfIndexIdGreaterThanN) throws Exception;

    Optional<MiruInvertedIndex<BM>> getInvertedIndex(MiruTermId term) throws Exception;

    MiruInvertedIndex<BM> getOrCreateInvertedIndex(MiruTermId term) throws Exception;
}
