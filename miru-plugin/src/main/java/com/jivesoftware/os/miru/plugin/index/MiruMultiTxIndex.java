package com.jivesoftware.os.miru.plugin.index;

import com.jivesoftware.os.filer.io.api.StackBuffer;

/**
 *
 */
public interface MiruMultiTxIndex<IBM> {

    void txIndex(MultiIndexTx<IBM> tx, StackBuffer stackBuffer) throws Exception;

}
