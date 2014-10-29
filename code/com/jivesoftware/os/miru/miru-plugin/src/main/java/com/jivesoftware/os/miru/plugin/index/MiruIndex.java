package com.jivesoftware.os.miru.plugin.index;

import com.google.common.base.Optional;
import com.jivesoftware.os.miru.api.base.MiruTermId;

/**
 *
 * @author jonathan
 */
public interface MiruIndex<BM> {

    Optional<MiruInvertedIndex<BM>> get(int fieldId, MiruTermId termId) throws Exception;

    Optional<MiruInvertedIndex<BM>> get(int fieldId, MiruTermId termId, int considerIfIndexIdGreaterThanN) throws Exception;

    MiruInvertedIndex<BM> allocate(int fieldId, MiruTermId termId) throws Exception;

    void index(int fieldId, MiruTermId termId, int... ids) throws Exception;

    void remove(int fieldId, MiruTermId termId, int id) throws Exception;

    long sizeInMemory() throws Exception;

    long sizeOnDisk() throws Exception;
}
