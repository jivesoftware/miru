package com.jivesoftware.os.miru.plugin.solution;

import com.google.common.base.Optional;
import com.jivesoftware.os.jive.utils.http.client.rest.NonSuccessStatusCodeException;
import com.jivesoftware.os.jive.utils.http.client.rest.RequestHelper;
import com.jivesoftware.os.miru.api.MiruQueryServiceException;
import com.jivesoftware.os.miru.api.activity.MiruPartitionId;
import com.jivesoftware.os.miru.plugin.partition.MiruPartitionUnavailableException;
import com.jivesoftware.os.mlogger.core.EndPointMetrics;
import org.apache.commons.httpclient.HttpStatus;

/**
 *
 */
public class MiruRemotePartitionReader<Q, A, P> {

    private final MiruRemotePartition<Q, A, P> remotePartition;
    private final RequestHelper requestHelper;

    public MiruRemotePartitionReader(MiruRemotePartition<Q, A, P> remotePartition, RequestHelper requestHelper) {
        this.remotePartition = remotePartition;
        this.requestHelper = requestHelper;
    }

    public MiruPartitionResponse<A> read(MiruPartitionId partitionId, MiruRequest<Q> request, Optional<P> report)
        throws MiruQueryServiceException, MiruPartitionUnavailableException {

        EndPointMetrics endpointMetrics = remotePartition.getEndpointMetrics();
        endpointMetrics.start();
        try {
            MiruRequestAndReport<Q, P> params = new MiruRequestAndReport<>(request, report.orNull());
            return requestHelper.executeRequest(params, remotePartition.getEndpoint(partitionId),
                MiruPartitionResponse.class, new Class[] { remotePartition.getAnswerClass() },
                new MiruPartitionResponse<>(remotePartition.getEmptyResults(), null));
        } catch (NonSuccessStatusCodeException e) {
            if (e.getStatusCode() == HttpStatus.SC_SERVICE_UNAVAILABLE) {
                throw new MiruPartitionUnavailableException("Remote partition is unavailable");
            } else {
                throw new MiruQueryServiceException("Failed remote read for partition: " + partitionId.getId(), e);
            }
        } catch (RuntimeException e) {
            throw new MiruQueryServiceException("Failed remote read for partition: " + partitionId.getId(), e);
        } finally {
            endpointMetrics.stop();
        }
    }
}
