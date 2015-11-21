package com.jivesoftware.os.miru.plugin.index;

import com.google.common.base.Optional;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

/**
 *
 * @author jonathan
 */
public interface MiruInvertedIndex<IBM> extends MiruInvertedIndexAppender, MiruTxIndex<IBM> {

    /**
     * Gets the raw index in a safe manner. The returned index is guaranteed to be free of concurrent writes,
     * so this method is safe to use even if the calling thread does not hold the context's write lock.
     *
     * @return the raw safe index
     * @throws Exception
     */
    Optional<IBM> getIndex(byte[] primitiveBuffer) throws Exception;

    /**
     * Gets the raw index in an unsafe manner. The returned index is not guaranteed to be free of concurrent writes,
     * so this method should only be used if the calling thread holds the context's write lock.
     *
     * @return the raw unsafe index
     * @throws Exception
     */
    Optional<IBM> getIndexUnsafe(byte[] primitiveBuffer) throws Exception;

    void replaceIndex(IBM index, int setLastId, byte[] primitiveBuffer) throws Exception;

    void remove(int id, byte[] primitiveBuffer) throws Exception;

    /**
     * Sets multiple bits anywhere in the bitmap.
     *
     * @param ids the indexes of the bits to set
     * @throws Exception
     */
    void set(byte[] primitiveBuffer, int... ids) throws Exception;

    int lastId(byte[] primitiveBuffer) throws Exception;

    void andNotToSourceSize(List<IBM> masks, byte[] primitiveBuffer) throws Exception;

    void orToSourceSize(IBM mask, byte[] primitiveBuffer) throws Exception;

    void andNot(IBM mask, byte[] primitiveBuffer) throws Exception;

    void or(IBM mask, byte[] primitiveBuffer) throws Exception;

}
