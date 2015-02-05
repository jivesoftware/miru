package com.jivesoftware.os.miru.service.index;

import com.jivesoftware.os.filer.io.Filer;
import com.jivesoftware.os.filer.io.FilerTransaction;
import java.io.IOException;

/**
 *
 */
public interface MiruFilerProvider {

    <R> R read(long initialCapacity, FilerTransaction<Filer, R> filerTransaction) throws IOException;

    <R> R writeNewReplace(long initialCapacity, FilerTransaction<Filer, R> filerTransaction) throws IOException;

    <R> R readWriteAutoGrow(long initialCapacity, FilerTransaction<Filer, R> filerTransaction) throws IOException;
}
