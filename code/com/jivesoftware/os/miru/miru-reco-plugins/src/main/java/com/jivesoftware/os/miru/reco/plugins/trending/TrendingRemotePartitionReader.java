package com.jivesoftware.os.miru.reco.plugins.trending;

import com.google.common.base.Optional;
import com.jivesoftware.os.jive.utils.http.client.rest.RequestHelper;
import com.jivesoftware.os.jive.utils.logger.EndPointMetrics;
import com.jivesoftware.os.jive.utils.logger.MetricLogger;
import com.jivesoftware.os.jive.utils.logger.MetricLoggerFactory;
import com.jivesoftware.os.miru.api.MiruQueryServiceException;
import com.jivesoftware.os.miru.api.activity.MiruPartitionId;
import com.jivesoftware.os.miru.query.solution.MiruPartitionResponse;
import com.jivesoftware.os.miru.query.solution.MiruRequest;
import com.jivesoftware.os.miru.query.solution.MiruRequestAndReport;

import static com.jivesoftware.os.miru.reco.plugins.trending.TrendingConstants.CUSTOM_QUERY_ENDPOINT;
import static com.jivesoftware.os.miru.reco.plugins.trending.TrendingConstants.TRENDING_PREFIX;

/**
 *
 */
public class TrendingRemotePartitionReader {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    private final EndPointMetrics processMetrics;
    private final RequestHelper requestHelper;

    public TrendingRemotePartitionReader(RequestHelper requestHelper) {
        this.requestHelper = requestHelper;
        this.processMetrics = new EndPointMetrics("process", LOG);
    }

    public MiruPartitionResponse<TrendingAnswer> scoreTrending(MiruPartitionId partitionId, MiruRequest<TrendingQuery> request, Optional<TrendingReport> report)
            throws MiruQueryServiceException {

        MiruRequestAndReport<TrendingQuery, TrendingReport> params = new MiruRequestAndReport<>(request, report.orNull());
        processMetrics.start();
        try {
            return requestHelper.executeRequest(params,
                    TRENDING_PREFIX + CUSTOM_QUERY_ENDPOINT + "/" + partitionId.getId(),
                    MiruPartitionResponse.class, new Class[]{TrendingAnswer.class}, new MiruPartitionResponse<>(TrendingAnswer.EMPTY_RESULTS, null));
        } catch (RuntimeException e) {
            throw new MiruQueryServiceException("Failed score trending stream for partition: " + partitionId.getId(), e);
        } finally {
            processMetrics.stop();
        }
    }

}
