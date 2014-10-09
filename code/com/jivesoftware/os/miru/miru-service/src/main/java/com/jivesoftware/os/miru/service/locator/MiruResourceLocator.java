package com.jivesoftware.os.miru.service.locator;

import com.jivesoftware.os.filer.io.ByteBufferBackedFiler;
import com.jivesoftware.os.filer.io.RandomAccessFiler;
import java.io.File;
import java.io.IOException;

/**
 *
 */
public interface MiruResourceLocator {

    File getFilerFile(MiruResourcePartitionIdentifier identifier, String name) throws IOException;

    RandomAccessFiler getRandomAccessFiler(MiruResourcePartitionIdentifier identifier, String name, String mode) throws IOException;

    ByteBufferBackedFiler getByteBufferBackedFiler(MiruResourcePartitionIdentifier identifier, String name, long length) throws IOException;

    File[] getMapDirectories(MiruResourcePartitionIdentifier identifier, String name) throws IOException;

    File[] getSwapDirectories(MiruResourcePartitionIdentifier identifier, String name) throws IOException;

    File[] getChunkDirectories(MiruResourcePartitionIdentifier identifier, String name) throws IOException;

    long getInitialChunkSize();

    void clean(MiruResourcePartitionIdentifier identifier) throws IOException;

    File[] getPartitionPaths(MiruResourcePartitionIdentifier identifier) throws IOException;
}
