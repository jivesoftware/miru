package com.jivesoftware.os.miru.service.stream;

import com.jivesoftware.os.filer.io.api.StackBuffer;
import com.jivesoftware.os.miru.api.MiruBackingStorage;
import com.jivesoftware.os.miru.api.MiruPartitionCoord;
import com.jivesoftware.os.miru.api.wal.MiruSipCursor;
import com.jivesoftware.os.miru.plugin.bitmap.MiruBitmaps;
import java.io.IOException;

/**
 *
 */
public interface MiruContextFactory<S extends MiruSipCursor<S>> {
        MiruBackingStorage findBackingStorage(MiruPartitionCoord coord) throws Exception;

    <BM extends IBM, IBM> MiruContext<IBM, S> allocate(MiruBitmaps<BM, IBM> bitmaps,
        MiruPartitionCoord coord,
        MiruBackingStorage storage,
        StackBuffer stackBuffer) throws Exception;

    <BM extends IBM, IBM> MiruContext<IBM, S> copy(MiruBitmaps<BM, IBM> bitmaps,
        MiruPartitionCoord coord,
        MiruContext<IBM, S> from,
        MiruBackingStorage toStorage,
        StackBuffer stackBuffer) throws Exception;

    void markStorage(MiruPartitionCoord coord, MiruBackingStorage marked) throws Exception;

    void cleanDisk(MiruPartitionCoord coord) throws IOException;

    <BM extends IBM, IBM> void close(MiruContext<BM, S> context, MiruBackingStorage storage) throws IOException;

    <BM extends IBM, IBM> void releaseCaches(MiruContext<BM, S> context, MiruBackingStorage storage) throws IOException;
}
