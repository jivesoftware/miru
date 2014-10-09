package com.jivesoftware.os.miru.service.index;

import com.jivesoftware.os.filer.io.Filer;
import java.io.File;
import java.io.IOException;

/**
 *
 */
public interface MiruFilerProvider {

    File getBackingFile() throws IOException;

    Filer getFiler(long length) throws IOException;
}
