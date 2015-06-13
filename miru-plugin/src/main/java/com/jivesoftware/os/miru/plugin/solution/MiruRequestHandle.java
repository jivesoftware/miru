package com.jivesoftware.os.miru.plugin.solution;

import com.jivesoftware.os.miru.api.MiruPartitionCoord;
import com.jivesoftware.os.miru.api.wal.MiruSipCursor;
import com.jivesoftware.os.miru.plugin.bitmap.MiruBitmaps;
import com.jivesoftware.os.miru.plugin.context.MiruRequestContext;
import com.jivesoftware.os.routing.bird.http.client.HttpClient;

/**
 *
 */
public interface MiruRequestHandle<BM, S extends MiruSipCursor<S>> extends AutoCloseable {

    MiruBitmaps<BM> getBitmaps();

    MiruRequestContext<BM, S> getRequestContext();

    boolean isLocal();

    boolean canBackfill();

    MiruPartitionCoord getCoord();

    HttpClient getHttpClient();
}
