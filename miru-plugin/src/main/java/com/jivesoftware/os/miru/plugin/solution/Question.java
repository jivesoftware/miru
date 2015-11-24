package com.jivesoftware.os.miru.plugin.solution;

import com.google.common.base.Optional;
import com.jivesoftware.os.miru.api.MiruHost;
import com.jivesoftware.os.miru.api.MiruQueryServiceException;
import com.jivesoftware.os.miru.api.activity.MiruPartitionId;

/**
 * @param <Q> query type
 * @param <A> answer type
 * @param <P> report type
 * @author jonathan
 */
public interface Question<Q, A, P> {

    <BM extends IBM, IBM> MiruPartitionResponse<A> askLocal(MiruRequestHandle<BM, IBM, ?> queryHandle, Optional<P> report) throws Exception;

    MiruPartitionResponse<A> askRemote(MiruHost host, MiruPartitionId partitionId, Optional<P> report) throws MiruQueryServiceException;

    Optional<P> createReport(Optional<A> answer);
}
