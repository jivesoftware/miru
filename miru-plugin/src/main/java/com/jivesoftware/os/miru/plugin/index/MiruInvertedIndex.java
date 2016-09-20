package com.jivesoftware.os.miru.plugin.index;

import com.google.common.base.Optional;
import com.jivesoftware.os.filer.io.api.StackBuffer;
import java.util.List;

/**
 * @author jonathan
 */
public interface MiruInvertedIndex<BM extends IBM, IBM> extends MiruInvertedIndexAppender, MiruTxIndex<IBM> {

    void getIndex(BitmapAndLastId<BM> container, StackBuffer stackBuffer) throws Exception;

    void remove(StackBuffer stackBuffer, int... ids) throws Exception;

    /**
     * Sets a single bit if the index has never been modified. NOTE: If the index is empty as a result of previous removals,
     * the index remains unchanged. Effectively, the index is modified if and only if {@link #lastId(StackBuffer)} returns -1.
     *
     * @param stackBuffer the stack buffer
     * @param id the index of the bit to set
     * @throws Exception
     */
    boolean setIfEmpty(StackBuffer stackBuffer, int id) throws Exception;

    /**
     * Returns the index of the highest bit ever set in the bitmap, even if that bit has been removed.
     *
     * @param stackBuffer the stack  buffer
     * @return the last id
     * @throws Exception
     */
    int lastId(StackBuffer stackBuffer) throws Exception;

    void andNotToSourceSize(List<IBM> masks, StackBuffer stackBuffer) throws Exception;

    void orToSourceSize(IBM mask, StackBuffer stackBuffer) throws Exception;

    void andNot(IBM mask, StackBuffer stackBuffer) throws Exception;

    void or(IBM mask, StackBuffer stackBuffer) throws Exception;

}
