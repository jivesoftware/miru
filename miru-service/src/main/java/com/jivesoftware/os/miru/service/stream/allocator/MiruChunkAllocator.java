package com.jivesoftware.os.miru.service.stream.allocator;

import com.jivesoftware.os.filer.io.api.StackBuffer;
import com.jivesoftware.os.filer.io.chunk.ChunkStore;
import com.jivesoftware.os.miru.api.MiruPartitionCoord;
import com.jivesoftware.os.miru.service.stream.KeyedIndexStore;
import java.io.IOException;

/**
 *
 */
public interface MiruChunkAllocator {

    boolean checkExists(MiruPartitionCoord coord) throws Exception;

    ChunkStore[] allocateChunkStores(MiruPartitionCoord coord, StackBuffer stackBuffer) throws Exception;

    KeyedIndexStore[] allocateIndexStores(MiruPartitionCoord coord) throws IOException;

    void close(ChunkStore[] chunkStores, KeyedIndexStore[] environments) throws IOException;
}
