package com.jivesoftware.os.miru.service.stream.locator;

import com.jivesoftware.os.jive.utils.io.ByteBufferBackedFiler;
import com.jivesoftware.os.jive.utils.io.FileBackedMemMappedByteBufferFactory;
import com.jivesoftware.os.jive.utils.io.RandomAccessFiler;
import com.jivesoftware.os.jive.utils.logger.MetricLogger;
import com.jivesoftware.os.jive.utils.logger.MetricLoggerFactory;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import org.apache.commons.io.FileUtils;

/**
 *
 */
public abstract class AbstractIdentifierPartResourceLocator implements MiruResourceLocator {

    protected static final MetricLogger log = MetricLoggerFactory.getLogger();

    protected final File basePath;
    protected final long initialChunkSize;

    public AbstractIdentifierPartResourceLocator(File basePath, long initialChunkSize) {
        this.basePath = basePath;
        this.initialChunkSize = initialChunkSize;
    }

    @Override
    public File getFilerFile(MiruResourcePartitionIdentifier identifier, String name) throws IOException {
        return new File(ensurePartitionPath(identifier), name + ".filer");
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
        ByteBuffer byteBuffer = bufferFactory.allocate(file.length());
        return new ByteBufferBackedFiler(file, byteBuffer);
    }

    @Override
    public File getMapDirectory(MiruResourcePartitionIdentifier identifier, String name) throws IOException {
        return ensureDirectory(new File(getMapPath(identifier), name));
    }

    @Override
    public File getSwapDirectory(MiruResourcePartitionIdentifier identifier, String name) throws IOException {
        return ensureDirectory(new File(getSwapPath(identifier), name));
    }

    @Override
    public File getChunkFile(MiruResourcePartitionIdentifier identifier, String name) throws IOException {
        return new File(ensurePartitionPath(identifier), name + ".chunk");
    }

    @Override
    public long getInitialChunkSize() {
        return initialChunkSize;
    }

    @Override
    public void clean(MiruResourcePartitionIdentifier identifier) throws IOException {
        FileUtil.remove(ensurePartitionPath(identifier));
    }

    @Override
    public File getPartitionPath(MiruResourcePartitionIdentifier identifier) {
        File path = basePath;
        for (String part : identifier.getParts()) {
            path = new File(path, part);
        }
        return path;
    }

    private File ensurePartitionPath(MiruResourcePartitionIdentifier identifier) {
        File path = basePath;
        for (String part : identifier.getParts()) {
            path = ensureDirectory(new File(path, part));
        }
        return path;
    }

    private File getMapPath(MiruResourcePartitionIdentifier identifier) {
        File partitionPath = ensurePartitionPath(identifier);
        return ensureDirectory(new File(partitionPath, "maps"));
    }

    private File getSwapPath(MiruResourcePartitionIdentifier identifier) {
        File partitionPath = ensurePartitionPath(identifier);
        return ensureDirectory(new File(partitionPath, "swaps"));
    }

    private File ensureDirectory(File file) {
        try {
            FileUtils.forceMkdir(file);
        } catch (IOException x) {
            log.error("Path should be a directory: {} exists:{} isDirectory:{} canRead:{} canWrite:{} canExecute:{}", new Object[] {
                file.getAbsolutePath(), file.exists(), file.isDirectory(), file.canRead(), file.canWrite(), file.canExecute() });
            throw new IllegalStateException("Failed to ensure directory", x);
        }
        return file;
    }
}
