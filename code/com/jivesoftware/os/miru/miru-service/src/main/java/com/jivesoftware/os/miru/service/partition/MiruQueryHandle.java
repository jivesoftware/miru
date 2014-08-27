package com.jivesoftware.os.miru.service.partition;

import com.jivesoftware.os.jive.utils.http.client.rest.RequestHelper;
import com.jivesoftware.os.miru.api.MiruPartitionCoord;
import com.jivesoftware.os.miru.service.stream.MiruQueryStream;

/**
 *
 */
public interface MiruQueryHandle extends AutoCloseable {

    MiruQueryStream getQueryStream();

    boolean isLocal();

    boolean canBackfill();

    MiruPartitionCoord getCoord();

    RequestHelper getRequestHelper();
}
