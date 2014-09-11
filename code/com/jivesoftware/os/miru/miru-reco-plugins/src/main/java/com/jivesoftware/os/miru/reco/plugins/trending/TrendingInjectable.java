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

    public MiruResponse<TrendingAnswer> scoreTrending(TrendingQuery query) throws MiruQueryServiceException {
        try {
            LOG.debug("askAndMerge: query={}", query);
            MiruTenantId tenantId = query.tenantId;
            Miru miru = miruProvider.getMiru(tenantId);
            return miru.askAndMerge(tenantId,
                    new MiruSolvableFactory<>("scoreTrending", new TrendingQuestion(trending, query)),
                    new TrendingAnswerEvaluator(),
                    new TrendingAnswerMerger(query.timeRange, query.divideTimeRangeIntoNSegments, query.desiredNumberOfDistincts),
                    TrendingAnswer.EMPTY_RESULTS);
        } catch (MiruPartitionUnavailableException e) {
            throw e;
        } catch (Exception e) {
            //TODO throw http error codes
            throw new MiruQueryServiceException("Failed to score trending stream", e);
        }
    }

    public TrendingAnswer scoreTrending(MiruPartitionId partitionId,
            TrendingQueryAndReport queryAndReport)
            throws MiruQueryServiceException {
        try {
            LOG.debug("askImmediate: partitionId={} query={}", partitionId, queryAndReport.query);
            LOG.trace("askImmediate: report={}", queryAndReport.report);
            MiruTenantId tenantId = queryAndReport.query.tenantId;
            Miru miru = miruProvider.getMiru(tenantId);
            return miru.askImmediate(tenantId,
                    partitionId,
                    new MiruSolvableFactory<>("scoreTrending", new TrendingQuestion(trending, queryAndReport.query)),
                    Optional.fromNullable(queryAndReport.report),
                    TrendingAnswer.EMPTY_RESULTS);
        } catch (MiruPartitionUnavailableException e) {
            throw e;
        } catch (Exception e) {
            //TODO throw http error codes
            throw new MiruQueryServiceException("Failed to score trending stream for partition: " + partitionId.getId(), e);
        }
    }

}
