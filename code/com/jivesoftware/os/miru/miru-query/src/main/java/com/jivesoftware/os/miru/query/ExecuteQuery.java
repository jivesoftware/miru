package com.jivesoftware.os.miru.query;

import com.google.common.base.Optional;
import com.jivesoftware.os.jive.utils.http.client.rest.RequestHelper;
import com.jivesoftware.os.miru.api.activity.MiruPartitionId;

/**
 * @param <R> result type
 * @param <P> report type
 * @author jonathan
 */
public interface ExecuteQuery<R, P> {

    <BM> R executeLocal(MiruQueryHandle<BM> queryHandle, Optional<P> report) throws Exception;

    R executeRemote(RequestHelper requestHelper, MiruPartitionId partitionId, Optional<R> lastResult) throws Exception;

    Optional<P> createReport(Optional<R> result);
}
