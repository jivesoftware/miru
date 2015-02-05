package com.jivesoftware.os.miru.analytics.plugins.metrics;

import com.google.common.base.Optional;
import com.jivesoftware.os.miru.api.MiruQueryServiceException;
import com.jivesoftware.os.miru.api.activity.MiruPartitionId;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.plugin.Miru;
import com.jivesoftware.os.miru.plugin.MiruProvider;
import com.jivesoftware.os.miru.plugin.partition.MiruPartitionUnavailableException;
import com.jivesoftware.os.miru.plugin.solution.MiruPartitionResponse;
import com.jivesoftware.os.miru.plugin.solution.MiruRequest;
import com.jivesoftware.os.miru.plugin.solution.MiruRequestAndReport;
import com.jivesoftware.os.miru.plugin.solution.MiruResponse;
import com.jivesoftware.os.miru.plugin.solution.MiruSolvableFactory;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;

/**
 *
 */
public class MetricsInjectable {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    private final MiruProvider<? extends Miru> miruProvider;
    private final Metrics trending;

    public MetricsInjectable(MiruProvider<? extends Miru> miruProvider, Metrics trending) {
        this.miruProvider = miruProvider;
        this.trending = trending;
    }

    public MiruResponse<MetricsAnswer> score(MiruRequest<MetricsQuery> request) throws MiruQueryServiceException {
        try {
            LOG.debug("askAndMerge: request={}", request);
            MiruTenantId tenantId = request.tenantId;
            Miru miru = miruProvider.getMiru(tenantId);
            return miru.askAndMerge(tenantId,
                new MiruSolvableFactory<>("score", new MetricsQuestion(trending, request)),
                new MetricsAnswerEvaluator(),
                new MetricsAnswerMerger(request.query.timeRange),
                MetricsAnswer.EMPTY_RESULTS,
                request.logLevel);
        } catch (MiruPartitionUnavailableException e) {
            throw e;
        } catch (Exception e) {
            //TODO throw http error codes
            throw new MiruQueryServiceException("Failed to score trending stream", e);
        }
    }

    public MiruPartitionResponse<MetricsAnswer> score(MiruPartitionId partitionId,
        MiruRequestAndReport<MetricsQuery, MetricsReport> requestAndReport) throws MiruQueryServiceException {
        try {
            LOG.debug("askImmediate: partitionId={} request={}", partitionId, requestAndReport.request);
            LOG.trace("askImmediate: report={}", requestAndReport.report);
            MiruTenantId tenantId = requestAndReport.request.tenantId;
            Miru miru = miruProvider.getMiru(tenantId);
            return miru.askImmediate(tenantId,
                partitionId,
                new MiruSolvableFactory<>("scoreTrending", new MetricsQuestion(trending, requestAndReport.request)),
                Optional.fromNullable(requestAndReport.report),
                MetricsAnswer.EMPTY_RESULTS,
                requestAndReport.request.logLevel);
        } catch (MiruPartitionUnavailableException e) {
            throw e;
        } catch (Exception e) {
            //TODO throw http error codes
            throw new MiruQueryServiceException("Failed to score trending stream for partition: " + partitionId.getId(), e);
        }
    }

}
