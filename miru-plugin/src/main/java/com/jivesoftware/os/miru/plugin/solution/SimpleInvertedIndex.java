package com.jivesoftware.os.miru.plugin.solution;

import com.jivesoftware.os.filer.io.api.StackBuffer;
import com.jivesoftware.os.miru.plugin.index.IndexTx;
import com.jivesoftware.os.miru.plugin.index.MiruTxIndex;

/**
 *
 */
public class SimpleInvertedIndex<IBM> implements MiruTxIndex<IBM> {

    private final IBM bitmap;

    public SimpleInvertedIndex(IBM bitmap) {
        this.bitmap = bitmap;
    }

    @Override
    public <R> R txIndex(IndexTx<R, IBM> tx, StackBuffer stackBuffer) throws Exception {
        return tx.tx(bitmap, null, -1, null);
    }
}
