package com.jivesoftware.os.miru.service.stream.factory;

import com.google.common.base.Optional;
import com.jivesoftware.os.jive.utils.http.client.rest.RequestHelper;
import com.jivesoftware.os.miru.api.activity.MiruPartitionId;
import com.jivesoftware.os.miru.service.partition.MiruQueryHandle;

/**
 * @param <R> result type
 * @param <P> report type
 * @author jonathan
 */
public interface ExecuteQuery<R, P> {

    R executeLocal(MiruQueryHandle queryHandle, Optional<P> report) throws Exception;

    R executeRemote(RequestHelper requestHelper, MiruPartitionId partitionId, Optional<R> lastResult) throws Exception;

    Optional<P> createReport(Optional<R> result);
}
