package com.jivesoftware.os.miru.reco.plugins.reco;

import com.google.common.base.Optional;
import com.jivesoftware.os.jive.utils.http.client.rest.RequestHelper;
import com.jivesoftware.os.jive.utils.logger.EndPointMetrics;
import com.jivesoftware.os.jive.utils.logger.MetricLogger;
import com.jivesoftware.os.jive.utils.logger.MetricLoggerFactory;
import com.jivesoftware.os.miru.api.MiruQueryServiceException;
import com.jivesoftware.os.miru.api.activity.MiruPartitionId;

import static com.jivesoftware.os.miru.reco.plugins.reco.RecoConstants.CUSTOM_QUERY_ENDPOINT;
import static com.jivesoftware.os.miru.reco.plugins.reco.RecoConstants.RECO_PREFIX;

/**
 *
 */
public class RecoRemotePartitionReader {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    private final EndPointMetrics processMetrics;
    private final RequestHelper requestHelper;

    public RecoRemotePartitionReader(RequestHelper requestHelper) {
        this.requestHelper = requestHelper;
        this.processMetrics = new EndPointMetrics("process", LOG);
    }

    public RecoResult collaborativeFilteringRecommendations(MiruPartitionId partitionId,
            RecoQuery query,
            Optional<RecoResult> lastResult)
            throws MiruQueryServiceException {
        RecoQueryAndResult params = new RecoQueryAndResult(query, lastResult.orNull());
        processMetrics.start();
        try {
            return requestHelper.executeRequest(params,
                    RECO_PREFIX + CUSTOM_QUERY_ENDPOINT + "/" + partitionId.getId(),
                    RecoResult.class, RecoResult.EMPTY_RESULTS);
        } catch (RuntimeException e) {
            throw new MiruQueryServiceException("Failed score reco stream for partition: " + partitionId.getId(), e);
        } finally {
            processMetrics.stop();
        }
    }
}
