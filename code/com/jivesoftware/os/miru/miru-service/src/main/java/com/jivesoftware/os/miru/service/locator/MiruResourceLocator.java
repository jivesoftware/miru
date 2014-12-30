package com.jivesoftware.os.miru.service.locator;

import java.io.File;
import java.io.IOException;

/**
 *
 */
public interface MiruResourceLocator {

    File getFilerFile(MiruResourcePartitionIdentifier identifier, String name) throws IOException;

    File[] getChunkDirectories(MiruResourcePartitionIdentifier identifier, String name) throws IOException;

    long getInitialChunkSize();

    void clean(MiruResourcePartitionIdentifier identifier) throws IOException;

    File[] getPartitionPaths(MiruResourcePartitionIdentifier identifier) throws IOException;
}
