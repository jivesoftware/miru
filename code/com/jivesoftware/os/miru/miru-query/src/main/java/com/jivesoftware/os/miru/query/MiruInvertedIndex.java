package com.jivesoftware.os.miru.query;

/**
 *
 * @author jonathan
 */
public interface MiruInvertedIndex<BM> extends MiruInvertedIndexAppender {

    BM getIndex() throws Exception;

    void remove(int id) throws Exception;

    void set(int id) throws Exception;

    void setIntermediate(int... ids) throws Exception;

    void andNotToSourceSize(BM mask) throws Exception;

    void orToSourceSize(BM mask) throws Exception;

    void andNot(BM mask) throws Exception;

    void or(BM mask) throws Exception;

    long sizeInMemory() throws Exception;

    long sizeOnDisk() throws Exception;
}
