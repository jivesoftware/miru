package com.jivesoftware.os.miru.plugin.index;

import com.jivesoftware.os.filer.io.api.StackBuffer;
import com.jivesoftware.os.filer.io.chunk.ChunkFiler;
import java.nio.ByteBuffer;

/**
 *
 */
public interface MiruTxIndex<IBM> {

    <R> R txIndex(IndexTx<R, IBM> tx, StackBuffer stackBuffer) throws Exception;

    interface IndexTx<R, IBM> {

        R tx(IBM bitmap, ChunkFiler filer, StackBuffer stackBuffer) throws Exception;
    }

}
