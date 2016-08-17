package com.jivesoftware.os.miru.service.locator;

import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import org.apache.commons.io.FileUtils;

/**
 *
 */
public class DiskIdentifierPartResourceLocator implements MiruResourceLocator {

    protected static final MetricLogger log = MetricLoggerFactory.getLogger();

    private static final String DISK_FORMAT_VERSION = "version-14";

    protected final File[] basePaths;
    protected final long onDiskInitialChunkSize;
    protected final long inMemoryChunkSize;

    public DiskIdentifierPartResourceLocator(File[] unversionedPaths, long onDiskInitialChunkSize, long inMemoryChunkSize) {
        this.basePaths = versionedPaths(unversionedPaths);
        this.onDiskInitialChunkSize = onDiskInitialChunkSize;
        this.inMemoryChunkSize = inMemoryChunkSize;
    }

    private static File[] versionedPaths(File[] basePaths) {
        File[] versionedPaths = new File[basePaths.length];
        for (int i = 0; i < basePaths.length; i++) {
            versionedPaths[i] = new File(basePaths[i], DISK_FORMAT_VERSION);
        }
        return versionedPaths;
    }

    private File getPartitionPathByName(MiruResourcePartitionIdentifier identifier, String name) {
        File[] partitionPaths = ensurePartitionPaths(identifier);
        return partitionPaths[Math.abs(name.hashCode() % partitionPaths.length)];
    }

    @Override
    public File getFilerFile(MiruResourcePartitionIdentifier identifier, String name) throws IOException {
        return new File(getPartitionPathByName(identifier, name), name + ".filer");
    }

    @Override
    public File[] getChunkDirectories(MiruResourcePartitionIdentifier identifier, String name, int version) {
        String chunkName = (version < 0) ? name : name + "-v" + version;
        return makeSubDirectories(ensurePartitionPaths(identifier), chunkName);
    }

    @Override
    public long getOnDiskInitialChunkSize() {
        return onDiskInitialChunkSize;
    }

    @Override
    public long getInMemoryChunkSize() {
        return inMemoryChunkSize;
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
