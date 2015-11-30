package com.jivesoftware.os.miru.plugin.index;

import com.jivesoftware.os.filer.io.api.KeyRange;
import com.jivesoftware.os.filer.io.api.StackBuffer;
import com.jivesoftware.os.miru.api.base.MiruTermId;
import java.util.Arrays;
import java.util.List;

/**
 * @author jonathan
 */
public interface MiruFieldIndex<IBM> {

    MiruInvertedIndex<IBM> get(int fieldId, MiruTermId termId) throws Exception;

    MiruInvertedIndex<IBM> get(int fieldId, MiruTermId termId, int considerIfIndexIdGreaterThanN) throws Exception;

    MiruInvertedIndex<IBM> getOrCreateInvertedIndex(int fieldId, MiruTermId term) throws Exception;

    void append(int fieldId, MiruTermId termId, int[] ids, long[] counts, StackBuffer stackBuffer) throws Exception;

    void set(int fieldId, MiruTermId termId, int[] ids, long[] counts, StackBuffer stackBuffer) throws Exception;

    void remove(int fieldId, MiruTermId termId, int id, StackBuffer stackBuffer) throws Exception;

    void streamTermIdsForField(int fieldId, List<KeyRange> ranges, TermIdStream termIdStream, StackBuffer stackBuffer) throws Exception;

    long getCardinality(int fieldId, MiruTermId termId, int id, StackBuffer stackBuffer) throws Exception;

    long[] getCardinalities(int fieldId, MiruTermId termId, int[] ids, StackBuffer stackBuffer) throws Exception;

    long getGlobalCardinality(int fieldId, MiruTermId termId, StackBuffer stackBuffer) throws Exception;

    void mergeCardinalities(int fieldId, MiruTermId termId, int[] ids, long[] counts, StackBuffer stackBuffer) throws Exception;

    class IndexKey {

        public final long id;
        public final byte[] keyBytes;

        public IndexKey(long id, byte[] keyBytes) {
            this.id = id;
            this.keyBytes = keyBytes;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            IndexKey indexKey = (IndexKey) o;

            if (id != indexKey.id) {
                return false;
            }
            if (!Arrays.equals(keyBytes, indexKey.keyBytes)) {
                return false;
            }

            return true;
        }

        @Override
        public int hashCode() {
            int result = (int) (id ^ (id >>> 32));
            result = 31 * result + (keyBytes != null ? Arrays.hashCode(keyBytes) : 0);
            return result;
        }
    }
}
