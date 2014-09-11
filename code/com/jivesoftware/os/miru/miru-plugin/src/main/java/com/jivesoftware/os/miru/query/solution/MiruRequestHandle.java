package com.jivesoftware.os.miru.query.solution;

import com.jivesoftware.os.jive.utils.http.client.rest.RequestHelper;
import com.jivesoftware.os.miru.api.MiruPartitionCoord;
import com.jivesoftware.os.miru.query.bitmap.MiruBitmaps;
import com.jivesoftware.os.miru.query.context.MiruRequestContext;

/**
 *
 */
public interface MiruRequestHandle<BM> extends AutoCloseable {

    MiruBitmaps<BM> getBitmaps();

    MiruRequestContext<BM> getRequestContext();

    boolean isLocal();

    boolean canBackfill();

    MiruPartitionCoord getCoord();

    RequestHelper getRequestHelper();
}
