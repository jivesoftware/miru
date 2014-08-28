package com.jivesoftware.os.miru.query;

import com.jivesoftware.os.jive.utils.http.client.rest.RequestHelper;
import com.jivesoftware.os.miru.api.MiruPartitionCoord;

/**
 *
 */
public interface MiruQueryHandle<BM> extends AutoCloseable {

    MiruBitmaps<BM> getBitmaps();

    MiruQueryStream<BM> getQueryStream();

    boolean isLocal();

    boolean canBackfill();

    MiruPartitionCoord getCoord();

    RequestHelper getRequestHelper();
}
