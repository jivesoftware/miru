package com.jivesoftware.os.miru.plugin.index;

import com.jivesoftware.os.filer.io.api.KeyRange;
import com.jivesoftware.os.filer.io.api.StackBuffer;
import com.jivesoftware.os.miru.api.base.MiruTermId;
import java.util.List;

/**
 * @author jonathan
 */
public interface MiruFieldIndex<BM extends IBM, IBM> {

    MiruInvertedIndex<BM, IBM> get(int fieldId, MiruTermId termId) throws Exception;

    MiruInvertedIndex<BM, IBM> getOrCreateInvertedIndex(int fieldId, MiruTermId term) throws Exception;

    void multiGet(int fieldId, MiruTermId[] termIds, BM[] results, StackBuffer stackBuffer) throws Exception;

    void multiTxIndex(int fieldId,
        MiruTermId[] termIds,
        int considerIfLastIdGreaterThanN,
        StackBuffer stackBuffer,
        MultiIndexTx<IBM> indexTx) throws Exception;

    void append(int fieldId, MiruTermId termId, int[] ids, long[] counts, StackBuffer stackBuffer) throws Exception;

    void set(int fieldId, MiruTermId termId, int[] ids, long[] counts, StackBuffer stackBuffer) throws Exception;

    void remove(int fieldId, MiruTermId termId, int id, StackBuffer stackBuffer) throws Exception;

    void streamTermIdsForField(int fieldId, List<KeyRange> ranges, TermIdStream termIdStream, StackBuffer stackBuffer) throws Exception;

    long getCardinality(int fieldId, MiruTermId termId, int id, StackBuffer stackBuffer) throws Exception;

    long[] getCardinalities(int fieldId, MiruTermId termId, int[] ids, StackBuffer stackBuffer) throws Exception;

    long getGlobalCardinality(int fieldId, MiruTermId termId, StackBuffer stackBuffer) throws Exception;

    void mergeCardinalities(int fieldId, MiruTermId termId, int[] ids, long[] counts, StackBuffer stackBuffer) throws Exception;

}
