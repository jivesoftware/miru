package com.jivesoftware.os.miru.plugin.solution;

import com.google.common.base.Optional;
import com.jivesoftware.os.miru.api.MiruQueryServiceException;
import com.jivesoftware.os.miru.api.activity.MiruPartitionId;
import com.jivesoftware.os.routing.bird.http.client.HttpClient;

/**
 * @param <Q> query type
 * @param <A> answer type
 * @param <P> report type
 * @author jonathan
 */
public interface Question<Q, A, P> {

    <BM> MiruPartitionResponse<A> askLocal(MiruRequestHandle<BM, ?> queryHandle, Optional<P> report) throws Exception;

    MiruPartitionResponse<A> askRemote(HttpClient httpClient, MiruPartitionId partitionId, Optional<P> report) throws MiruQueryServiceException;

    Optional<P> createReport(Optional<A> answer);
}
