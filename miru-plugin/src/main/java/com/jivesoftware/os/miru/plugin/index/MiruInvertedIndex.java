package com.jivesoftware.os.miru.plugin.index;

import com.google.common.base.Optional;
import java.io.IOException;
import java.nio.ByteBuffer;
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
    Optional<BM> getIndex(byte[] primitiveBuffer) throws Exception;

    <R> R txIndex(IndexTx<R, BM> tx, byte[] primitiveBuffer) throws Exception;

    interface IndexTx<R, BM> {

        R tx(BM bitmap, ByteBuffer buffer) throws Exception;
    }

    /**
     * Gets the raw index in an unsafe manner. The returned index is not guaranteed to be free of concurrent writes,
     * so this method should only be used if the calling thread holds the context's write lock.
     *
     * @return the raw unsafe index
     * @throws Exception
     */
    Optional<BM> getIndexUnsafe(byte[] primitiveBuffer) throws Exception;

    void replaceIndex(BM index, int setLastId, byte[] primitiveBuffer) throws Exception;

    void remove(int id, byte[] primitiveBuffer) throws Exception;

    /**
     * Sets multiple bits anywhere in the bitmap.
     *
     * @param ids the indexes of the bits to set
     * @throws Exception
     */
    void set(byte[] primitiveBuffer, int... ids) throws Exception;

    int lastId(byte[] primitiveBuffer) throws Exception;

    void andNotToSourceSize(List<BM> masks, byte[] primitiveBuffer) throws Exception;

    void orToSourceSize(BM mask, byte[] primitiveBuffer) throws Exception;

    void andNot(BM mask, byte[] primitiveBuffer) throws Exception;

    void or(BM mask, byte[] primitiveBuffer) throws Exception;

}
