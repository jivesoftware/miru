package com.jivesoftware.os.miru.plugin.index;

import com.google.common.base.Optional;
import com.jivesoftware.os.miru.api.base.MiruTermId;
import java.util.Iterator;

/**
 *
 * @author jonathan
 */
public interface MiruFieldIndex<BM> {

    Optional<MiruInvertedIndex<BM>> get(int fieldId, MiruTermId termId) throws Exception;

    Optional<MiruInvertedIndex<BM>> get(int fieldId, MiruTermId termId, int considerIfIndexIdGreaterThanN) throws Exception;

    MiruInvertedIndex<BM> getOrCreateInvertedIndex(int fieldId, MiruTermId term) throws Exception;

    void index(int fieldId, MiruTermId termId, int... ids) throws Exception;

    void remove(int fieldId, MiruTermId termId, int id) throws Exception;

    Iterator<MiruTermId> getTermIdsForField(int fieldId) throws Exception;

    long sizeInMemory() throws Exception;

    long sizeOnDisk() throws Exception;
}
