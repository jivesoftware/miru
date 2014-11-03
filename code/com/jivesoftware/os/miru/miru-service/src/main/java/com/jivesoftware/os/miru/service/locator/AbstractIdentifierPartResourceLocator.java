package com.jivesoftware.os.miru.service.locator;

import com.jivesoftware.os.filer.io.ByteBufferBackedFiler;
import com.jivesoftware.os.filer.io.FileBackedMemMappedByteBufferFactory;
import com.jivesoftware.os.filer.io.RandomAccessFiler;
import com.jivesoftware.os.jive.utils.logger.MetricLogger;
import com.jivesoftware.os.jive.utils.logger.MetricLoggerFactory;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import org.apache.commons.io.FileUtils;

/**
 *
 */
public abstract class AbstractIdentifierPartResourceLocator implements MiruResourceLocator {

    protected static final MetricLogger log = MetricLoggerFactory.getLogger();

    protected final File[] basePaths;
    protected final long initialChunkSize;

    public AbstractIdentifierPartResourceLocator(File[] basePaths, long initialChunkSize) {
        this.basePaths = basePaths;
        this.initialChunkSize = initialChunkSize;
    }

    private File getPartitionPathByName(MiruResourcePartitionIdentifier identifier, String name) {
        File[] partitionPaths = ensurePartitionPaths(identifier);
        return partitionPaths[Math.abs(name.hashCode()) % partitionPaths.length];
    }

    @Override
    public File getFilerFile(MiruResourcePartitionIdentifier identifier, String name) throws IOException {
        return new File(getPartitionPathByName(identifier, name), name + ".filer");
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

        FileBackedMemMappedByteBufferFactory bufferFactory = new FileBackedMemMappedByteBufferFactory(file.getParentFile());
        ByteBuffer byteBuffer = bufferFactory.allocate(file.getName(), Math.max(file.length(), length));
        return new ByteBufferBackedFiler(file, byteBuffer);
    }

    @Override
    public File[] getMapDirectories(MiruResourcePartitionIdentifier identifier, String name) throws IOException {
        return ensureDirectories(makeSubDirectories(getMapPaths(identifier), name));
    }

    @Override
    public File[] getSwapDirectories(MiruResourcePartitionIdentifier identifier, String name) throws IOException {
        return ensureDirectories(makeSubDirectories(getSwapPaths(identifier), name));
    }

    @Override
    public File[] getChunkDirectories(MiruResourcePartitionIdentifier identifier, String name) {
        return makeSubDirectories(ensurePartitionPaths(identifier), name);
    }

    @Override
    public long getInitialChunkSize() {
        return initialChunkSize;
    }

    @Override
    public void clean(MiruResourcePartitionIdentifier identifier) throws IOException {
        for (File partitionPath : ensurePartitionPaths(identifier)) {
            FileUtil.remove(partitionPath);
        }
    }

    @Override
    public File[] getPartitionPaths(MiruResourcePartitionIdentifier identifier) {
        File[] paths = Arrays.copyOf(basePaths, basePaths.length);
        for (String part : identifier.getParts()) {
            paths = makeSubDirectories(paths, part);
        }
        return paths;
    }

    private File[] ensurePartitionPaths(MiruResourcePartitionIdentifier identifier) {
        File[] paths = Arrays.copyOf(basePaths, basePaths.length);
        for (String part : identifier.getParts()) {
            paths = ensureDirectories(makeSubDirectories(paths, part));
        }
        return paths;
    }

    private File[] getMapPaths(MiruResourcePartitionIdentifier identifier) {
        File[] partitionPaths = ensurePartitionPaths(identifier);
        return ensureDirectories(makeSubDirectories(partitionPaths, "maps"));
    }

    private File[] getSwapPaths(MiruResourcePartitionIdentifier identifier) {
        File[] partitionPaths = ensurePartitionPaths(identifier);
        return ensureDirectories(makeSubDirectories(partitionPaths, "swaps"));
    }

    private File[] makeSubDirectories(File[] baseDirectories, String subName) {
        File[] subDirectories = new File[baseDirectories.length];
        for (int i = 0; i < subDirectories.length; i++) {
            subDirectories[i] = new File(baseDirectories[i], subName);
        }
        return subDirectories;
    }

    private File[] ensureDirectories(File[] files) {
        for (File file : files) {
            try {
                FileUtils.forceMkdir(file);
            } catch (IOException x) {
                log.error("Path should be a directory: {} exists:{} isDirectory:{} canRead:{} canWrite:{} canExecute:{}",
                    file.getAbsolutePath(), file.exists(), file.isDirectory(), file.canRead(), file.canWrite(), file.canExecute());
                throw new IllegalStateException("Failed to ensure directory", x);
            }
        }
        return files;
    }
}
