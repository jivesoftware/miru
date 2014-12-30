package com.jivesoftware.os.miru.service.index;

import com.jivesoftware.os.filer.io.Filer;
import com.jivesoftware.os.filer.io.FilerTransaction;
import java.io.IOException;

/**
 *
 */
public interface MiruFilerProvider {

    <R> R execute(long initialCapacity, FilerTransaction<Filer, R> filerTransaction) throws IOException;
}
