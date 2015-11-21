package com.jivesoftware.os.miru.plugin.index;

import java.nio.ByteBuffer;

/**
 *
 */
public interface MiruTxIndex<IBM> {

    <R> R txIndex(IndexTx<R, IBM> tx, byte[] primitiveBuffer) throws Exception;

    interface IndexTx<R, IBM> {

        R tx(IBM bitmap, ByteBuffer buffer) throws Exception;
    }

}
