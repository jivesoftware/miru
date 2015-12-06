package com.jivesoftware.os.miru.plugin.index;

import com.jivesoftware.os.filer.io.api.StackBuffer;

/**
 *
 */
public interface MiruTxIndex<IBM> {

    <R> R txIndex(IndexTx<R, IBM> tx, StackBuffer stackBuffer) throws Exception;

}
