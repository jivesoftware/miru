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

    private final MiruProvider<? extends Miru> provider;
    private final Metrics metrics;

    public MetricsInjectable(MiruProvider<? extends Miru> miruProvider,
        Metrics trending) {
        this.provider = miruProvider;
        this.metrics = trending;
    }

    public MiruResponse<MetricsAnswer> score(MiruRequest<MetricsQuery> request) throws MiruQueryServiceException, InterruptedException {
        try {
            LOG.debug("askAndMerge: request={}", request);
            MiruTenantId tenantId = request.tenantId;
            Miru miru = provider.getMiru(tenantId);
            return miru.askAndMerge(tenantId,
                new MiruSolvableFactory<>(provider.getStats(), "metrics", new MetricsQuestion(metrics,
                    request,
                    provider.getRemotePartition(MetricsRemotePartition.class))),
                new MetricsAnswerEvaluator(),
                new MetricsAnswerMerger(request.query.timeRange, request.query.divideTimeRangeIntoNSegments),
                MetricsAnswer.EMPTY_RESULTS,
                request.logLevel);
        } catch (MiruPartitionUnavailableException | InterruptedException e) {
            throw e;
        } catch (Exception e) {
            //TODO throw http error codes
            throw new MiruQueryServiceException("Failed to score trending stream", e);
        }
    }

    public MiruPartitionResponse<MetricsAnswer> score(MiruPartitionId partitionId,
        MiruRequestAndReport<MetricsQuery, MetricsReport> requestAndReport) throws MiruQueryServiceException, InterruptedException {
        try {
            LOG.debug("askImmediate: partitionId={} request={}", partitionId, requestAndReport.request);
            LOG.trace("askImmediate: report={}", requestAndReport.report);
            MiruTenantId tenantId = requestAndReport.request.tenantId;
            Miru miru = provider.getMiru(tenantId);
            return miru.askImmediate(tenantId,
                partitionId,
                new MiruSolvableFactory<>(provider.getStats(), "metrics", new MetricsQuestion(metrics,
                    requestAndReport.request,
                    provider.getRemotePartition(MetricsRemotePartition.class))),
                Optional.fromNullable(requestAndReport.report),
                MetricsAnswer.EMPTY_RESULTS,
                requestAndReport.request.logLevel);
        } catch (MiruPartitionUnavailableException | InterruptedException e) {
            throw e;
        } catch (Exception e) {
            //TODO throw http error codes
            throw new MiruQueryServiceException("Failed to score trending stream for partition: " + partitionId.getId(), e);
        }
    }

}
