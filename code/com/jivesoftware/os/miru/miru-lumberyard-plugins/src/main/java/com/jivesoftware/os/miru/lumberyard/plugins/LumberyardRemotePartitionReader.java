package com.jivesoftware.os.miru.lumberyard.plugins;

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

import static com.jivesoftware.os.miru.lumberyard.plugins.LumberyardConstants.CUSTOM_QUERY_ENDPOINT;
import static com.jivesoftware.os.miru.lumberyard.plugins.LumberyardConstants.LUMBERYARD_PREFIX;

/**
 *
 */
public class LumberyardRemotePartitionReader {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    private final EndPointMetrics processMetrics;
    private final RequestHelper requestHelper;

    public LumberyardRemotePartitionReader(RequestHelper requestHelper) {
        this.requestHelper = requestHelper;
        this.processMetrics = new EndPointMetrics("process", LOG);
    }

    public MiruPartitionResponse<LumberyardAnswer> scoreLumberyarding(MiruPartitionId partitionId, MiruRequest<LumberyardQuery> request,
        Optional<LumberyardReport> report) throws MiruQueryServiceException {

        MiruRequestAndReport<LumberyardQuery, LumberyardReport> params = new MiruRequestAndReport<>(request, report.orNull());
        processMetrics.start();
        try {
            return requestHelper.executeRequest(params,
                LUMBERYARD_PREFIX + CUSTOM_QUERY_ENDPOINT + "/" + partitionId.getId(),
                MiruPartitionResponse.class, new Class[]{LumberyardAnswer.class}, new MiruPartitionResponse<>(LumberyardAnswer.EMPTY_RESULTS, null));
        } catch (RuntimeException e) {
            throw new MiruQueryServiceException("Failed score lumberyard for partition: " + partitionId.getId(), e);
        } finally {
            processMetrics.stop();
        }
    }

}
