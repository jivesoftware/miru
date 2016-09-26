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
        boolean fsyncOnAppend,
        MiruResourceLocator resourceLocator,
        MiruChunkAllocator chunkAllocator) throws Exception {

        File[] labDirs = resourceLocator.getChunkDirectories(() -> new String[] { "timeId" }, "lab", -1);
        for (File labDir : labDirs) {
            labDir.mkdirs();
        }
        LABEnvironment[] labEnvironments = chunkAllocator.allocateTimeIdLABEnvironments(labDirs);
        LabTimeIdIndex[] timeIdIndexes = new LabTimeIdIndex[labEnvironments.length];
        for (int i = 0; i < labEnvironments.length; i++) {
            timeIdIndexes[i] = new LabTimeIdIndex(labEnvironments[i], keepNIndexes, maxEntriesPerIndex, maxHeapPressureInBytes, fsyncOnAppend);
        }
        return timeIdIndexes;
    }
}
