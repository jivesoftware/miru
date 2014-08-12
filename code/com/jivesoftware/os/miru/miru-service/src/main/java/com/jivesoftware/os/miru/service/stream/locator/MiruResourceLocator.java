package com.jivesoftware.os.miru.service.stream.locator;

import com.jivesoftware.os.jive.utils.io.ByteBufferBackedFiler;
import com.jivesoftware.os.jive.utils.io.RandomAccessFiler;
import java.io.File;
import java.io.IOException;

/**
 *
 */
public interface MiruResourceLocator {

    File getFilerFile(MiruResourcePartitionIdentifier identifier, String name) throws IOException;

    RandomAccessFiler getRandomAccessFiler(MiruResourcePartitionIdentifier identifier, String name, String mode) throws IOException;

    ByteBufferBackedFiler getByteBufferBackedFiler(MiruResourcePartitionIdentifier identifier, String name, long length) throws IOException;

    File getMapDirectory(MiruResourcePartitionIdentifier identifier, String name) throws IOException;

    File getSwapDirectory(MiruResourcePartitionIdentifier identifier, String name) throws IOException;

    File getChunkFile(MiruResourcePartitionIdentifier identifier, String name) throws IOException;

    long getInitialChunkSize();

    void clean(MiruResourcePartitionIdentifier identifier) throws IOException;

    File getPartitionPath(MiruResourcePartitionIdentifier identifier) throws IOException;
}
