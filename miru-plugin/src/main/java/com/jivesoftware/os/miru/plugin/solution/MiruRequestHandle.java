package com.jivesoftware.os.miru.plugin.solution;

import com.jivesoftware.os.miru.api.MiruPartitionCoord;
import com.jivesoftware.os.miru.api.wal.MiruSipCursor;
import com.jivesoftware.os.miru.plugin.bitmap.MiruBitmaps;
import com.jivesoftware.os.miru.plugin.context.MiruRequestContext;
import com.jivesoftware.os.miru.plugin.partition.TrackError;
import java.util.concurrent.ExecutorService;

/**
 *
 */
public interface MiruRequestHandle<BM extends IBM, IBM, S extends MiruSipCursor<S>> extends AutoCloseable {

    MiruBitmaps<BM, IBM> getBitmaps();

    MiruRequestContext<BM, IBM, S> getRequestContext();

    boolean isLocal();

    boolean canBackfill();

    MiruPartitionCoord getCoord();

    TrackError getTrackError();

    void submit(ExecutorService executorService, AsyncQuestion<BM, IBM> asyncQuestion);

    void acquireChitsAndMerge(String name, long batchSize) throws Exception;

    void compact() throws Exception;

    interface AsyncQuestion<BM extends IBM, IBM> {

        void ask(MiruRequestHandle<BM, IBM, ?> handle) throws Exception;
    }
}
