package com.jivesoftware.os.miru.service.index.lab;

import com.jivesoftware.os.lab.LABEnvironment;
import com.jivesoftware.os.lab.guts.LABHashIndexType;

/**
 *
 */
public class LabTimeIdIndexInitializer {

    public LabTimeIdIndex[] initialize(int keepNIndexes,
        int maxEntriesPerIndex,
        long maxHeapPressureInBytes,
        LABHashIndexType hashIndexType,
        double hashIndexLoadFactor,
        boolean hashIndexEnabled,
        boolean fsyncOnAppend,
        boolean verboseLogging,
        LABEnvironment[] labEnvironments) throws Exception {

        LabTimeIdIndex[] timeIdIndexes = new LabTimeIdIndex[labEnvironments.length];
        for (int i = 0; i < labEnvironments.length; i++) {
            timeIdIndexes[i] = new LabTimeIdIndex(labEnvironments[i],
                keepNIndexes,
                maxEntriesPerIndex,
                maxHeapPressureInBytes,
                hashIndexType,
                hashIndexLoadFactor,
                hashIndexEnabled,
                fsyncOnAppend,
                verboseLogging);
        }
        return timeIdIndexes;
    }
}
