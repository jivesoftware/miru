package com.jivesoftware.os.miru.service.index;

import com.googlecode.javaewah.EWAHCompressedBitmap;

/**
 *
 * @author jonathan
 */
public interface MiruInvertedIndex extends MiruInvertedIndexAppender {

    EWAHCompressedBitmap getIndex() throws Exception;

    void remove(int id) throws Exception;

    void set(int id) throws Exception;

    void andNotToSourceSize(EWAHCompressedBitmap mask) throws Exception;

    void orToSourceSize(EWAHCompressedBitmap mask) throws Exception;

    void andNot(EWAHCompressedBitmap mask) throws Exception;

    void or(EWAHCompressedBitmap mask) throws Exception;

    long sizeInMemory() throws Exception;

    long sizeOnDisk() throws Exception;

}
