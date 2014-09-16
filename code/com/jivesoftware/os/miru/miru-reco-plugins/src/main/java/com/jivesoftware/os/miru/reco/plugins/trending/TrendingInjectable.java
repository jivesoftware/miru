package com.jivesoftware.os.miru.reco.plugins.trending;

import com.google.common.base.Optional;
import com.jivesoftware.os.jive.utils.logger.MetricLogger;
import com.jivesoftware.os.jive.utils.logger.MetricLoggerFactory;
import com.jivesoftware.os.miru.api.MiruQueryServiceException;
import com.jivesoftware.os.miru.api.activity.MiruPartitionId;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.query.Miru;
import com.jivesoftware.os.miru.query.MiruProvider;
import com.jivesoftware.os.miru.query.partition.MiruPartitionUnavailableException;
import com.jivesoftware.os.miru.query.solution.MiruPartitionResponse;
import com.jivesoftware.os.miru.query.solution.MiruRequest;
import com.jivesoftware.os.miru.query.solution.MiruRequestAndReport;
import com.jivesoftware.os.miru.query.solution.MiruResponse;
import com.jivesoftware.os.miru.query.solution.MiruSolvableFactory;

/**
 *
 */
public class TrendingInjectable {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    private final MiruProvider<? extends Miru> miruProvider;
    private final Trending trending;

    public TrendingInjectable(MiruProvider<? extends Miru> miruProvider, Trending trending) {
        this.miruProvider = miruProvider;
        this.trending = trending;
    }

    public MiruResponse<TrendingAnswer> scoreTrending(MiruRequest<TrendingQuery> request) throws MiruQueryServiceException {
        try {
            LOG.debug("askAndMerge: request={}", request);
            MiruTenantId tenantId = request.tenantId;
            Miru miru = miruProvider.getMiru(tenantId);
            return miru.askAndMerge(tenantId,
                    new MiruSolvableFactory<>("scoreTrending", new TrendingQuestion(trending, request)),
                    new TrendingAnswerEvaluator(),
                    new TrendingAnswerMerger(request.query.timeRange, request.query.divideTimeRangeIntoNSegments, request.query.desiredNumberOfDistincts),
                    TrendingAnswer.EMPTY_RESULTS, request.debug);
        } catch (MiruPartitionUnavailableException e) {
            throw e;
        } catch (Exception e) {
            //TODO throw http error codes
            throw new MiruQueryServiceException("Failed to score trending stream", e);
        }
    }

    public MiruPartitionResponse<TrendingAnswer> scoreTrending(MiruPartitionId partitionId,
            MiruRequestAndReport<TrendingQuery,TrendingReport> requestAndReport)
            throws MiruQueryServiceException {
        try {
            LOG.debug("askImmediate: partitionId={} request={}", partitionId, requestAndReport.request);
            LOG.trace("askImmediate: report={}", requestAndReport.report);
            MiruTenantId tenantId = requestAndReport.request.tenantId;
            Miru miru = miruProvider.getMiru(tenantId);
            return miru.askImmediate(tenantId,
                    partitionId,
                    new MiruSolvableFactory<>("scoreTrending", new TrendingQuestion(trending, requestAndReport.request)),
                    Optional.fromNullable(requestAndReport.report),
                    TrendingAnswer.EMPTY_RESULTS, false);
        } catch (MiruPartitionUnavailableException e) {
            throw e;
        } catch (Exception e) {
            //TODO throw http error codes
            throw new MiruQueryServiceException("Failed to score trending stream for partition: " + partitionId.getId(), e);
        }
    }

}
