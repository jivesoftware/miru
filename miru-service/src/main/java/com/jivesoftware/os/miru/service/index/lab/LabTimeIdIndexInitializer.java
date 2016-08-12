package com.jivesoftware.os.miru.service.index.lab;

import com.jivesoftware.os.lab.LABEnvironment;
import com.jivesoftware.os.miru.service.locator.MiruResourceLocator;
import com.jivesoftware.os.miru.service.stream.allocator.MiruChunkAllocator;
import java.io.File;

/**
 *
 */
public class LabTimeIdIndexInitializer {

    public LabTimeIdIndex[] initialize(int keepNIndexes,
        int maxEntriesPerIndex,
        long maxHeapPressureInBytes,
        MiruResourceLocator resourceLocator,
        MiruChunkAllocator chunkAllocator) throws Exception {

        File[] dirs = resourceLocator.getChunkDirectories(() -> new String[] { "timeId" }, "lab");
        LABEnvironment[] labEnvironments = chunkAllocator.allocateLABEnvironments(dirs);
        LabTimeIdIndex[] timeIdIndexes = new LabTimeIdIndex[labEnvironments.length];
        for (int i = 0; i < labEnvironments.length; i++) {
            timeIdIndexes[i] = new LabTimeIdIndex(labEnvironments[i], keepNIndexes, maxEntriesPerIndex, maxHeapPressureInBytes);
        }
        return timeIdIndexes;
    }
}
