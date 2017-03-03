package com.jivesoftware.os.miru.service.stream.allocator;

import com.jivesoftware.os.filer.io.chunk.ChunkStore;
import com.jivesoftware.os.lab.LABEnvironment;
import com.jivesoftware.os.miru.api.MiruPartitionCoord;
import java.io.File;

/**
 *
 */
public interface MiruChunkAllocator {

    boolean checkExists(MiruPartitionCoord coord, int labVersion, int[] supportedLabVersions) throws Exception;

    boolean hasChunkStores(MiruPartitionCoord coord) throws Exception;

    boolean hasLabIndex(MiruPartitionCoord coord, int version) throws Exception;

    ChunkStore[] allocateChunkStores(MiruPartitionCoord coord) throws Exception;

    void close(ChunkStore[] chunkStores);

    void remove(ChunkStore[] chunkStores);

    void close(LABEnvironment[] labEnvironments) throws Exception;

    File[] getLabDirs(MiruPartitionCoord coord, int version) throws Exception;

    LABEnvironment[] allocateLABEnvironments(MiruPartitionCoord coord, int version) throws Exception;

    LABEnvironment[] allocateLABEnvironments(File[] labDirs) throws Exception;

    LABEnvironment[] allocateTimeIdLABEnvironments(File[] labDirs) throws Exception;

    void remove(LABEnvironment[] labEnvironments);
}
