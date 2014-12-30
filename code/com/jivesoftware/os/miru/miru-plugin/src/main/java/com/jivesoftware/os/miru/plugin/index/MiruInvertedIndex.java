package com.jivesoftware.os.miru.plugin.index;

import com.google.common.base.Optional;
import java.util.List;

/**
 *
 * @author jonathan
 */
public interface MiruInvertedIndex<BM> extends MiruInvertedIndexAppender {

    /**
     * Gets the raw index in a safe manner. The returned index is guaranteed to be free of concurrent writes,
     * so this method is safe to use even if the calling thread does not hold the context's write lock.
     *
     * @return the raw safe index
     * @throws Exception
     */
    Optional<BM> getIndex() throws Exception;

    /**
     * Gets the raw index in an unsafe manner. The returned index is not guaranteed to be free of concurrent writes,
     * so this method should only be used if the calling thread holds the context's write lock.
     *
     * @return the raw unsafe index
     * @throws Exception
     */
    Optional<BM> getIndexUnsafe() throws Exception;

    void remove(int id) throws Exception;

    /**
     * Sets multiple bits at the end of the bitmap, in ascending order.
     *
     * @param ids the indexes of the bits to set, in ascending order
     * @throws Exception
     */
    void set(int... ids) throws Exception;

    /**
     * Sets multiple bits anywhere in the bitmap.
     *
     * @param ids the indexes of the bits to set
     * @throws Exception
     */
    void setIntermediate(int... ids) throws Exception;

    int lastId() throws Exception;

    void andNotToSourceSize(List<BM> masks) throws Exception;

    void orToSourceSize(BM mask) throws Exception;

    void andNot(BM mask) throws Exception;

    void or(BM mask) throws Exception;

    long sizeInMemory() throws Exception;

    long sizeOnDisk() throws Exception;
}
