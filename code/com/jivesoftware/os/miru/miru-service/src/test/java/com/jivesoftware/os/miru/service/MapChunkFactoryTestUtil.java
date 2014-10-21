package com.jivesoftware.os.miru.service;

import com.jivesoftware.os.filer.map.store.FileBackedMapChunkFactory;
import com.jivesoftware.os.filer.map.store.MapChunkFactory;
import java.io.IOException;
import java.nio.file.Files;

/**
 *
 */
public class MapChunkFactoryTestUtil {

    private MapChunkFactoryTestUtil() {}

    public static MapChunkFactory createFileBackedMapChunkFactory(String name,
        int keySize,
        boolean variableKeySize,
        int payloadSize,
        boolean variablePayloadSize,
        int initialPageCapacity,
        int numDirs)
        throws IOException {

        String[] pathsToPartitions = new String[numDirs];
        for (int i = 0; i < numDirs; i++) {
            pathsToPartitions[i] = Files.createTempDirectory(name).toFile().getAbsolutePath();
        }
        return new FileBackedMapChunkFactory(keySize, variableKeySize, payloadSize, variablePayloadSize, initialPageCapacity, pathsToPartitions);
    }
}
