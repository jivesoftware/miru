package com.jivesoftware.os.miru.service.index;

import com.google.common.base.Optional;
import com.jivesoftware.os.miru.api.activity.MiruFieldDefinition;
import com.jivesoftware.os.miru.api.base.MiruTermId;

/** @author jonathan */
public interface MiruField<BM> {

    MiruFieldDefinition getFieldDefinition();

    long sizeInMemory() throws Exception;

    long sizeOnDisk() throws Exception;

    void index(MiruTermId term, int id) throws Exception;

    void remove(MiruTermId term, int id) throws Exception;

    Optional<MiruInvertedIndex<BM>> getInvertedIndex(MiruTermId term, int considerIfIndexIdGreaterThanN) throws Exception;

    Optional<MiruInvertedIndex<BM>> getInvertedIndex(MiruTermId term) throws Exception;

    MiruInvertedIndex<BM> getOrCreateInvertedIndex(MiruTermId term) throws Exception;

}
