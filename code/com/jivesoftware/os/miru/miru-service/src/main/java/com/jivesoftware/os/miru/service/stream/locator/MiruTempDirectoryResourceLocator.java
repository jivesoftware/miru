package com.jivesoftware.os.miru.service.stream.locator;

import com.google.common.base.Joiner;
import com.google.common.collect.Maps;
import com.google.common.io.BaseEncoding;
import com.jivesoftware.os.jive.utils.io.ByteBufferBackedFiler;
import com.jivesoftware.os.jive.utils.io.FileBackedMemMappedByteBufferFactory;
import com.jivesoftware.os.jive.utils.io.RandomAccessFiler;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.util.Random;
import java.util.concurrent.ConcurrentMap;

/**
 *
 */
public class MiruTempDirectoryResourceLocator implements MiruHybridResourceLocator {

    private final ConcurrentMap<MiruResourcePartitionIdentifier, File> partitionPaths = Maps.newConcurrentMap();
    private final Random random = new Random();

    @Override
    public MiruResourcePartitionIdentifier acquire() throws IOException {
        byte[] bytes = new byte[12];
        random.nextBytes(bytes);
        return new MiruHybridTokenIdentifier(BaseEncoding.base64Url().omitPadding().encode(bytes));
    }

    @Override
    public void release(MiruResourcePartitionIdentifier identifier) {
        // ignore
    }

    @Override
    public File getFilerFile(MiruResourcePartitionIdentifier identifier, String name) throws IOException {
        return new File(getPartitionPath(identifier), name + ".filer");
    }

    @Override
    public RandomAccessFiler getRandomAccessFiler(MiruResourcePartitionIdentifier identifier, String name, String mode) throws IOException {
        File file = getFilerFile(identifier, name);
        file.createNewFile();
        return new RandomAccessFiler(file, mode);
    }

    @Override
    public ByteBufferBackedFiler getByteBufferBackedFiler(MiruResourcePartitionIdentifier identifier, String name, long length) throws IOException {
        File file = getFilerFile(identifier, name);
        file.createNewFile();

        FileBackedMemMappedByteBufferFactory bufferFactory = new FileBackedMemMappedByteBufferFactory(file);
        ByteBuffer byteBuffer = bufferFactory.allocate(length);
        return new ByteBufferBackedFiler(file, byteBuffer);
    }

    @Override
    public File getMapDirectory(MiruResourcePartitionIdentifier identifier, String name) throws IOException {
        return new File(getMapPath(identifier), name);
    }

    @Override
    public File getSwapDirectory(MiruResourcePartitionIdentifier identifier, String name) throws IOException {
        return new File(getSwapPath(identifier), name);
    }

    @Override
    public File getChunkFile(MiruResourcePartitionIdentifier identifier, String name) throws IOException {
        return new File(getPartitionPath(identifier), name + ".chunk");
    }

    @Override
    public long getInitialChunkSize() {
        return 4096;
    }

    @Override
    public void clean(MiruResourcePartitionIdentifier identifier) throws IOException {
        FileUtil.remove(getPartitionPath(identifier));
    }

    @Override
    public File getPartitionPath(MiruResourcePartitionIdentifier identifier) throws IOException {
        synchronized (partitionPaths) {
            File partitionPath = partitionPaths.get(identifier);
            if (partitionPath == null || !partitionPath.exists()) {
                partitionPath = Files.createTempDirectory(Joiner.on('.').join(identifier.getParts())).toFile();
                partitionPaths.put(identifier, partitionPath);
            }
            return partitionPath;
        }
    }

    private File getMapPath(MiruResourcePartitionIdentifier identifier) throws IOException {
        return new File(getPartitionPath(identifier), "maps");
    }

    private File getSwapPath(MiruResourcePartitionIdentifier identifier) throws IOException {
        return new File(getPartitionPath(identifier), "swaps");
    }

}
