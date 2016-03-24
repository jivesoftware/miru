package com.jivesoftware.os.miru.plugin.index;

import com.jivesoftware.os.filer.io.Filer;
import com.jivesoftware.os.filer.io.api.StackBuffer;

/**
 *
 */
public interface IndexTx<R, IBM> {

    R tx(IBM bitmap, Filer filer, int offset, StackBuffer stackBuffer) throws Exception;
}
