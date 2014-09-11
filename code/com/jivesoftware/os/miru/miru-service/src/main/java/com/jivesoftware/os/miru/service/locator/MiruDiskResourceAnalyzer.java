package com.jivesoftware.os.miru.service.locator;

import java.io.File;
import java.util.List;

/**
 *
 */
public class MiruDiskResourceAnalyzer {

    public boolean checkExists(File partitionPath, List<String> filerNames, List<String> mapNames, List<String> chunkNames) {
        File mapPath = new File(partitionPath, "maps");
        if (!mapPath.exists()) {
            return false;
        }

        for (String filerName : filerNames) {
            File filer = new File(partitionPath, filerName + ".filer");
            if (!filer.exists()) {
                return false;
            }
        }

        for (String mapName : mapNames) {
            File mapDirectory = new File(mapPath, mapName);
            if (!mapDirectory.exists()) {
                return false;
            }
        }

        for (String chunkName : chunkNames) {
            File chunk = new File(partitionPath, chunkName + ".chunk");
            if (!chunk.exists()) {
                return false;
            }
        }

        return true;
    }
}
