package com.jivesoftware.os.miru.query;

import com.google.common.base.Optional;
import com.jivesoftware.os.jive.utils.http.client.rest.RequestHelper;
import com.jivesoftware.os.miru.api.activity.MiruPartitionId;

/**
 * @param <A> answer type
 * @param <P> report type
 * @author jonathan
 */
public interface Question<A, P> {

    <BM> A askLocal(MiruQueryHandle<BM> queryHandle, Optional<P> report) throws Exception;

    A askRemote(RequestHelper requestHelper, MiruPartitionId partitionId, Optional<A> lastAnswer) throws Exception;

    Optional<P> createReport(Optional<A> answer);
}
