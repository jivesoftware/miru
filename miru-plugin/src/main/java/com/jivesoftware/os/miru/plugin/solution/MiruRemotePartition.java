package com.jivesoftware.os.miru.plugin.solution;

import com.google.common.base.Optional;
import com.jivesoftware.os.miru.api.MiruHost;
import com.jivesoftware.os.miru.api.MiruQueryServiceException;
import com.jivesoftware.os.miru.api.activity.MiruPartitionId;
import com.jivesoftware.os.routing.bird.http.client.TenantAwareHttpClient;

/**
 *
 */
public interface MiruRemotePartition<Q, A, P> {

    MiruPartitionResponse<A> askRemote(MiruHost host,
        MiruPartitionId partitionId,
        MiruRequest<Q> request,
        Optional<P> report) throws MiruQueryServiceException;
}
