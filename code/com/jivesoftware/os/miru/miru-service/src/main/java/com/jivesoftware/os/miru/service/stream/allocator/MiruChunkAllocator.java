package com.jivesoftware.os.miru.service.stream.allocator;

import com.jivesoftware.os.filer.chunk.store.ChunkStore;
import com.jivesoftware.os.miru.api.MiruPartitionCoord;

/**
 *
 */
public interface MiruChunkAllocator {

    boolean checkExists(MiruPartitionCoord coord) throws Exception;

    ChunkStore[] allocateChunkStores(MiruPartitionCoord coord) throws Exception;

    <BM> void close(ChunkStore[] chunkStores);
}
