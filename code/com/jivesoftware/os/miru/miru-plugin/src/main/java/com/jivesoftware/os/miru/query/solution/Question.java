package com.jivesoftware.os.miru.query.solution;

import com.google.common.base.Optional;
import com.jivesoftware.os.jive.utils.http.client.rest.RequestHelper;
import com.jivesoftware.os.miru.api.activity.MiruPartitionId;

/**
 * @param <A> answer type
 * @param <P> report type
 * @author jonathan
 */
public interface Question<A, P> {

    <BM> MiruPartitionResponse<A> askLocal(MiruRequestHandle<BM> queryHandle, Optional<P> report) throws Exception;

    MiruPartitionResponse<A> askRemote(RequestHelper requestHelper, MiruPartitionId partitionId, Optional<P> report) throws Exception;

    Optional<P> createReport(Optional<A> answer);
}
