package com.jivesoftware.os.miru.plugin.solution;

import com.google.common.base.Optional;

/**
 * @param <Q> query type
 * @param <A> answer type
 * @param <P> report type
 * @author jonathan
 */
public interface Question<Q, A, P> {

    <BM> MiruPartitionResponse<A> askLocal(MiruRequestHandle<BM> queryHandle, Optional<P> report) throws Exception;

    MiruRemotePartition<Q, A, P> getRemotePartition();

    Optional<P> createReport(Optional<A> answer);

    MiruRequest<Q> getRequest();
}
