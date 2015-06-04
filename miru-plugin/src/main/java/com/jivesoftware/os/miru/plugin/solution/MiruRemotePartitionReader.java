package com.jivesoftware.os.miru.plugin.solution;

import com.google.common.base.Optional;
import com.jivesoftware.os.jive.utils.http.client.HttpClient;
import com.jivesoftware.os.miru.api.MiruQueryServiceException;
import com.jivesoftware.os.miru.plugin.partition.MiruPartitionUnavailableException;
import com.jivesoftware.os.mlogger.core.EndPointMetrics;

/**
 *
 */
public interface MiruRemotePartitionReader {

    <Q, A, P> MiruPartitionResponse<A> read(HttpClient httpClient,
        String endpoint,
        MiruRequest<Q> request,
        Class<A> answerClass,
        Optional<P> report,
        EndPointMetrics endPointMetrics,
        A emptyResults)
        throws MiruQueryServiceException, MiruPartitionUnavailableException;
}
