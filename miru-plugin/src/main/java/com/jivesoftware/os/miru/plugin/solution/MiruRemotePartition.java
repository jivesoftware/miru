package com.jivesoftware.os.miru.plugin.solution;

import com.google.common.base.Optional;
import com.jivesoftware.os.jive.utils.http.client.HttpClient;
import com.jivesoftware.os.miru.api.MiruQueryServiceException;
import com.jivesoftware.os.miru.api.activity.MiruPartitionId;
import com.jivesoftware.os.mlogger.core.EndPointMetrics;

/**
 *
 */
public interface MiruRemotePartition<Q, A, P> {

    MiruPartitionResponse<A> askRemote(HttpClient httpClient, MiruPartitionId partitionId, MiruRequest<Q> request, Optional<P> report) throws MiruQueryServiceException;
}
