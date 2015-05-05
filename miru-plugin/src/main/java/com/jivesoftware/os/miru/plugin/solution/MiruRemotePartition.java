package com.jivesoftware.os.miru.plugin.solution;

import com.jivesoftware.os.miru.api.activity.MiruPartitionId;
import com.jivesoftware.os.mlogger.core.EndPointMetrics;

/**
 *
 */
public interface MiruRemotePartition<Q, A, P> {

    String getEndpoint(MiruPartitionId partitionId);

    Class<A> getAnswerClass();

    A getEmptyResults();

    EndPointMetrics getEndpointMetrics();
}
