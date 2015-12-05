package com.jivesoftware.os.miru.plugin.index;

import com.google.common.base.Optional;
import com.jivesoftware.os.filer.io.api.StackBuffer;
import java.util.List;

/**
 *
 * @author jonathan
 */
public interface MiruInvertedIndex<BM extends IBM, IBM> extends MiruInvertedIndexAppender, MiruGetIndex<BM>, MiruTxIndex<IBM> {

    /**
     * Gets the raw index in an unsafe manner. The returned index is not guaranteed to be free of concurrent writes,
     * so this method should only be used if the calling thread holds the context's write lock.
     *
     * @return the raw unsafe index
     * @throws Exception
     */
    Optional<BM> getIndexUnsafe(StackBuffer stackBuffer) throws Exception;

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
