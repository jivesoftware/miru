package com.jivesoftware.os.miru.service.stream;

import com.google.common.primitives.UnsignedBytes;
import com.jivesoftware.os.miru.api.base.MiruIBA;
import java.io.IOException;
import java.util.Comparator;

/**
 *
 */
public interface KeyedIndexStore {

    void close();

    void delete() throws IOException;

    void flush(boolean fsync);

    KeyedIndex open(String indexName);

    void copyTo(KeyedIndexStore indexStore);
}
