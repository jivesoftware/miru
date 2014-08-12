package com.jivesoftware.os.miru.service.partition;

import com.jivesoftware.os.miru.api.activity.MiruPartitionId;
import com.jivesoftware.os.miru.service.stream.MiruQueryStream;

/**
 *
 */
public interface MiruQueryHandle extends AutoCloseable {

    MiruQueryStream getQueryStream();

    boolean canBackfill();

    MiruPartitionId getPartitionId();
}
