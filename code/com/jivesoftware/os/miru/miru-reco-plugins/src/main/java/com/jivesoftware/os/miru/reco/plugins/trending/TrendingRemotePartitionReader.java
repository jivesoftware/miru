package com.jivesoftware.os.miru.reco.plugins.trending;

import com.google.common.base.Optional;
import com.jivesoftware.os.jive.utils.http.client.rest.RequestHelper;
import com.jivesoftware.os.jive.utils.logger.EndPointMetrics;
import com.jivesoftware.os.jive.utils.logger.MetricLogger;
import com.jivesoftware.os.jive.utils.logger.MetricLoggerFactory;
import com.jivesoftware.os.miru.api.MiruQueryServiceException;
import com.jivesoftware.os.miru.api.activity.MiruPartitionId;

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

    public TrendingAnswer scoreTrending(MiruPartitionId partitionId, TrendingQuery query, Optional<TrendingReport> report)
            throws MiruQueryServiceException {

        TrendingQueryAndReport params = new TrendingQueryAndReport(query, report.orNull());
        processMetrics.start();
        try {
            return requestHelper.executeRequest(params,
                    TRENDING_PREFIX + CUSTOM_QUERY_ENDPOINT + "/" + partitionId.getId(),
                    TrendingAnswer.class, TrendingAnswer.EMPTY_RESULTS);
        } catch (RuntimeException e) {
            throw new MiruQueryServiceException("Failed score trending stream for partition: " + partitionId.getId(), e);
        } finally {
            processMetrics.stop();
        }
    }

}
