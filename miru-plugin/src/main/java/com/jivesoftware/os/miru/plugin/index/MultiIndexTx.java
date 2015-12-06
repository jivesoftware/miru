package com.jivesoftware.os.miru.plugin.index;

import com.jivesoftware.os.filer.io.api.StackBuffer;
import com.jivesoftware.os.filer.io.chunk.ChunkFiler;

/**
 *
 */
public interface MultiIndexTx<IBM> {

    void tx(int index, IBM bitmap, ChunkFiler filer, int offset, StackBuffer stackBuffer) throws Exception;
}
