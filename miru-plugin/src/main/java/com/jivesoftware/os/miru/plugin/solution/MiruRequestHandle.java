package com.jivesoftware.os.miru.plugin.solution;

import com.jivesoftware.os.jive.utils.http.client.HttpClient;
import com.jivesoftware.os.miru.api.MiruPartitionCoord;
import com.jivesoftware.os.miru.plugin.bitmap.MiruBitmaps;
import com.jivesoftware.os.miru.plugin.context.MiruRequestContext;

/**
 *
 */
public interface MiruRequestHandle<BM> extends AutoCloseable {

    MiruBitmaps<BM> getBitmaps();

    MiruRequestContext<BM> getRequestContext();

    boolean isLocal();

    boolean canBackfill();

    MiruPartitionCoord getCoord();

    HttpClient getHttpClient();
}
