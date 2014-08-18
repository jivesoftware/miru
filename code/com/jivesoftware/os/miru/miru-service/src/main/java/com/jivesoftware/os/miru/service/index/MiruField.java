package com.jivesoftware.os.miru.service.index;

import com.google.common.base.Optional;
import com.jivesoftware.os.miru.api.base.MiruTermId;

/** @author jonathan */
public interface MiruField {

    long sizeInMemory() throws Exception;

    long sizeOnDisk() throws Exception;

    void index(MiruTermId term, int id) throws Exception;

    void remove(MiruTermId term, int id) throws Exception;

    Optional<MiruInvertedIndex> getInvertedIndex(MiruTermId term, int considerIfIndexIdGreaterThanN) throws Exception;

    Optional<MiruInvertedIndex> getInvertedIndex(MiruTermId term) throws Exception;

    Optional<MiruInvertedIndex> getOrCreateInvertedIndex(MiruTermId term) throws Exception;

}
