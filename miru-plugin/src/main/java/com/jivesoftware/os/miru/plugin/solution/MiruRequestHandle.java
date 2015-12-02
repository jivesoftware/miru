package com.jivesoftware.os.miru.plugin.solution;

import com.jivesoftware.os.miru.api.MiruPartitionCoord;
import com.jivesoftware.os.miru.api.wal.MiruSipCursor;
import com.jivesoftware.os.miru.plugin.bitmap.MiruBitmaps;
import com.jivesoftware.os.miru.plugin.context.MiruRequestContext;
import com.jivesoftware.os.miru.plugin.partition.TrackError;

/**
 *
 */
public interface MiruRequestHandle<BM extends IBM, IBM, S extends MiruSipCursor<S>> extends AutoCloseable {

    MiruBitmaps<BM, IBM> getBitmaps();

    MiruRequestContext<IBM, S> getRequestContext();

    boolean isLocal();

    boolean canBackfill();

    MiruPartitionCoord getCoord();

    TrackError getTrackError();
}
