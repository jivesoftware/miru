package com.jivesoftware.os.miru.service.locator;

import java.io.File;
import java.util.List;

/**
 *
 */
public class MiruDiskResourceAnalyzer {

    public boolean checkExists(File[] partitionPaths, List<String> filerNames, List<String> mapNames) {
        for (File partitionPath : partitionPaths) {
            File mapPath = new File(partitionPath, "maps");
            if (!mapPath.exists() || !mapPath.isDirectory()) {
                return false;
            }

            for (String mapName : mapNames) {
                File mapDirectory = new File(mapPath, mapName);
                if (!mapDirectory.exists() || !mapDirectory.isDirectory()) {
                    return false;
                }
            }
        }

        for (String filerName : filerNames) {
            //TODO leaky
            File partitionPath = partitionPaths[Math.abs(filerName.hashCode()) % partitionPaths.length];
            File filer = new File(partitionPath, filerName + ".filer");
            if (!filer.exists()) {
                return false;
            }
        }

        return true;
    }
}
