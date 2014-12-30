package com.jivesoftware.os.miru.service.index.memory;

import com.jivesoftware.os.miru.plugin.index.MiruInvertedIndex;
import java.io.IOException;

/**
 *
 */
public interface KeyedInvertedIndexStream<BM> {
    boolean stream(byte[] key, MiruInvertedIndex<BM> invertedIndex) throws IOException;
}
