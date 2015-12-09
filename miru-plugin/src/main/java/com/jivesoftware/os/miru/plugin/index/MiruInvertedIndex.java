package com.jivesoftware.os.miru.plugin.index;

import com.google.common.base.Optional;
import com.jivesoftware.os.filer.io.api.StackBuffer;
import java.util.List;

/**
 * @author jonathan
 */
public interface MiruInvertedIndex<BM extends IBM, IBM> extends MiruInvertedIndexAppender, MiruTxIndex<IBM> {

    Optional<BM> getIndex(StackBuffer stackBuffer) throws Exception;

    Optional<BM> getIndex(int considerIfLastIdGreaterThanN, StackBuffer stackBuffer) throws Exception;

    void replaceIndex(IBM index, int setLastId, StackBuffer stackBuffer) throws Exception;

    void remove(int id, StackBuffer stackBuffer) throws Exception;

    /**
     * Sets multiple bits anywhere in the bitmap.
     *
     * @param ids the indexes of the bits to set
     * @throws Exception
     */
    void set(StackBuffer stackBuffer, int... ids) throws Exception;

    int lastId(StackBuffer stackBuffer) throws Exception;

    void andNotToSourceSize(List<IBM> masks, StackBuffer stackBuffer) throws Exception;

    void orToSourceSize(IBM mask, StackBuffer stackBuffer) throws Exception;

    void andNot(IBM mask, StackBuffer stackBuffer) throws Exception;

    void or(IBM mask, StackBuffer stackBuffer) throws Exception;

}
