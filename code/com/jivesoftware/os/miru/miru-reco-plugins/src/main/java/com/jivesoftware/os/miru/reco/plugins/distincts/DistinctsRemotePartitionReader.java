package com.jivesoftware.os.miru.reco.plugins.distincts;

import com.google.common.base.Optional;
import com.jivesoftware.os.jive.utils.http.client.rest.RequestHelper;
import com.jivesoftware.os.miru.api.MiruQueryServiceException;
import com.jivesoftware.os.miru.api.activity.MiruPartitionId;
import com.jivesoftware.os.miru.plugin.solution.MiruPartitionResponse;
import com.jivesoftware.os.miru.plugin.solution.MiruRequest;
import com.jivesoftware.os.miru.plugin.solution.MiruRequestAndReport;
import com.jivesoftware.os.mlogger.core.EndPointMetrics;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;

import static com.jivesoftware.os.miru.reco.plugins.distincts.DistinctsConstants.CUSTOM_QUERY_ENDPOINT;
import static com.jivesoftware.os.miru.reco.plugins.distincts.DistinctsConstants.DISTINCTS_PREFIX;

/**
 *
 */
public class DistinctsRemotePartitionReader {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    private final EndPointMetrics processMetrics;
    private final RequestHelper requestHelper;

    public DistinctsRemotePartitionReader(RequestHelper requestHelper) {
        this.requestHelper = requestHelper;
        this.processMetrics = new EndPointMetrics("process", LOG);
    }

    public MiruPartitionResponse<DistinctsAnswer> gatherDistincts(MiruPartitionId partitionId,
        MiruRequest<DistinctsQuery> request,
        Optional<DistinctsReport> report)
        throws MiruQueryServiceException {

        MiruRequestAndReport<DistinctsQuery, DistinctsReport> params = new MiruRequestAndReport<>(request, report.orNull());
        processMetrics.start();
        try {
            return requestHelper.executeRequest(params,
                DISTINCTS_PREFIX + CUSTOM_QUERY_ENDPOINT + "/" + partitionId.getId(),
                MiruPartitionResponse.class, new Class[] { DistinctsAnswer.class }, new MiruPartitionResponse<>(DistinctsAnswer.EMPTY_RESULTS, null));
        } catch (RuntimeException e) {
            throw new MiruQueryServiceException("Failed gather distincts for partition: " + partitionId.getId(), e);
        } finally {
            processMetrics.stop();
        }
    }

}
