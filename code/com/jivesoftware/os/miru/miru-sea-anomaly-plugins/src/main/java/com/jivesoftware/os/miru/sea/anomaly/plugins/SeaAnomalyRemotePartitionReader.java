package com.jivesoftware.os.miru.sea.anomaly.plugins;

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

import static com.jivesoftware.os.miru.sea.anomaly.plugins.SeaAnomalyConstants.CUSTOM_QUERY_ENDPOINT;

/**
 *
 */
public class SeaAnomalyRemotePartitionReader {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    private final EndPointMetrics processMetrics;
    private final RequestHelper requestHelper;

    public SeaAnomalyRemotePartitionReader(RequestHelper requestHelper) {
        this.requestHelper = requestHelper;
        this.processMetrics = new EndPointMetrics("process", LOG);
    }

    public MiruPartitionResponse<SeaAnomalyAnswer> scoreStumptowning(MiruPartitionId partitionId, MiruRequest<SeaAnomalyQuery> request,
        Optional<StumptownReport> report) throws MiruQueryServiceException {

        MiruRequestAndReport<SeaAnomalyQuery, StumptownReport> params = new MiruRequestAndReport<>(request, report.orNull());
        processMetrics.start();
        try {
            return requestHelper.executeRequest(params,
                SeaAnomalyConstants.SEA_ANOMALY_PREFIX + CUSTOM_QUERY_ENDPOINT + "/" + partitionId.getId(),
                MiruPartitionResponse.class, new Class[]{SeaAnomalyAnswer.class}, new MiruPartitionResponse<>(SeaAnomalyAnswer.EMPTY_RESULTS, null));
        } catch (RuntimeException e) {
            throw new MiruQueryServiceException("Failed score stumptown for partition: " + partitionId.getId(), e);
        } finally {
            processMetrics.stop();
        }
    }

}
