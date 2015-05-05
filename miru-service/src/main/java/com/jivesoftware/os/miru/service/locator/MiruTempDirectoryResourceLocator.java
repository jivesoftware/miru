package com.jivesoftware.os.miru.service.locator;

import com.google.common.base.Joiner;
import com.google.common.collect.Maps;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.concurrent.ConcurrentMap;

/**
 *
 */
public class MiruTempDirectoryResourceLocator implements MiruResourceLocator {

    private final ConcurrentMap<MiruResourcePartitionIdentifier, File> partitionPaths = Maps.newConcurrentMap();

    @Override
    public File getFilerFile(MiruResourcePartitionIdentifier identifier, String name) throws IOException {
        File[] partitionPaths = getPartitionPaths(identifier);
        //TODO leaky
        return new File(partitionPaths[Math.abs(name.hashCode()) % partitionPaths.length], name + ".filer");
    }

    @Override
    public File[] getChunkDirectories(MiruResourcePartitionIdentifier identifier, String name) throws IOException {
        return makeSubDirectories(getPartitionPaths(identifier), name);
    }

    @Override
    public long getOnDiskInitialChunkSize() {
        return 4_096;
    }

    @Override
    public long getInMemoryChunkSize() {
        return 4_096;
    }

    @Override
    public void clean(MiruResourcePartitionIdentifier identifier) throws IOException {
        for (File file : getPartitionPaths(identifier)) {
            FileUtil.remove(file);
        }
    }

    @Override
    public File[] getPartitionPaths(MiruResourcePartitionIdentifier identifier) throws IOException {
        synchronized (partitionPaths) {
            File partitionPath = partitionPaths.get(identifier);
            if (partitionPath == null || !partitionPath.exists()) {
                partitionPath = Files.createTempDirectory(Joiner.on('.').join(identifier.getParts())).toFile();
                partitionPaths.put(identifier, partitionPath);
            }
            return new File[] { partitionPath };
        }
    }

    private File[] makeSubDirectories(File[] baseDirectories, String subName) {
        File[] subDirectories = new File[baseDirectories.length];
        for (int i = 0; i < subDirectories.length; i++) {
            subDirectories[i] = new File(baseDirectories[i], subName);
        }
        return subDirectories;
    }

}
