package com.jivesoftware.os.miru.service.index;

import com.google.common.base.Optional;

/**
 *
 * @author jonathan
 */
public interface MiruIndex {

    Optional<MiruInvertedIndex> get(int fieldId, int termId) throws Exception;

    void allocate(int fieldId, int termId) throws Exception;

    void index(int fieldId, int termId, int id) throws Exception;

    void remove(int fieldId, int termId, int id) throws Exception;

    long sizeInMemory() throws Exception;

    long sizeOnDisk() throws Exception;
}
