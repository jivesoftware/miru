package com.jivesoftware.os.miru.plugin.index;

import com.jivesoftware.os.filer.io.Filer;
import com.jivesoftware.os.miru.api.wal.MiruSipCursor;
import java.io.IOException;

/**
 *
 */
public interface MiruSipIndexMarshaller<S extends MiruSipCursor<S>> {

    byte[] getSipIndexKey();

    S fromFiler(Filer filer, byte[] primitiveBuffer) throws IOException;

    void toFiler(Filer filer, S sip, byte[] primitiveBuffer) throws IOException;

    long expectedCapacity(S sip);
}
