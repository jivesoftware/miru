package com.jivesoftware.os.miru.plugin.index;

import com.jivesoftware.os.filer.io.api.KeyRange;
import com.jivesoftware.os.filer.io.api.StackBuffer;
import com.jivesoftware.os.miru.api.base.MiruTermId;
import java.util.List;

/**
 * @author jonathan
 */
public interface MiruFieldIndex<BM extends IBM, IBM> {

    MiruInvertedIndex<BM, IBM> get(String name, int fieldId, MiruTermId termId) throws Exception;

    MiruInvertedIndex<BM, IBM> getOrCreateInvertedIndex(String name, int fieldId, MiruTermId term) throws Exception;

    void multiGet(String name, int fieldId, MiruTermId[] termIds, BitmapAndLastId<BM>[] results, StackBuffer stackBuffer) throws Exception;

    void multiGetLastIds(String name, int fieldId, MiruTermId[] termIds, int[] lastIds, StackBuffer stackBuffer) throws Exception;

    void multiTxIndex(String name,
        int fieldId,
        MiruTermId[] termIds,
        int considerIfLastIdGreaterThanN,
        StackBuffer stackBuffer,
        MultiIndexTx<IBM> indexTx) throws Exception;

    void set(int fieldId, MiruTermId termId, int[] ids, long[] counts, StackBuffer stackBuffer) throws Exception;

    void setIfEmpty(int fieldId, MiruTermId termId, int id, long count, StackBuffer stackBuffer) throws Exception;

    void remove(int fieldId, MiruTermId termId, int[] ids, StackBuffer stackBuffer) throws Exception;

    void streamTermIdsForField(String name, int fieldId, List<KeyRange> ranges, TermIdStream termIdStream, StackBuffer stackBuffer) throws Exception;

    long getCardinality(int fieldId, MiruTermId termId, int id, StackBuffer stackBuffer) throws Exception;

    long[] getCardinalities(int fieldId, MiruTermId termId, int[] ids, StackBuffer stackBuffer) throws Exception;

    long getGlobalCardinality(int fieldId, MiruTermId termId, StackBuffer stackBuffer) throws Exception;

}
