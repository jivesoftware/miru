package com.jivesoftware.os.miru.analytics.plugins.analytics;

import com.google.common.base.Optional;
import com.google.common.collect.Maps;
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
import java.util.Map;

/**
 *
 */
public class AnalyticsInjectable {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    private final MiruProvider<? extends Miru> miruProvider;
    private final Analytics trending;

    public AnalyticsInjectable(MiruProvider<? extends Miru> miruProvider,
        Analytics trending) {
        this.miruProvider = miruProvider;
        this.trending = trending;
    }

    public MiruResponse<AnalyticsAnswer> score(MiruRequest<AnalyticsQuery> request) throws MiruQueryServiceException, InterruptedException {
        try {
            LOG.debug("askAndMerge: request={}", request);
            MiruTenantId tenantId = request.tenantId;
            Miru miru = miruProvider.getMiru(tenantId);

            Map<String, Integer> keyedSegments = Maps.newHashMap();
            for (AnalyticsQueryScoreSet scoreSet : request.query.scoreSets) {
                keyedSegments.put(scoreSet.key, scoreSet.divideTimeRangeIntoNSegments);
            }

            return miru.askAndMerge(tenantId,
                new MiruSolvableFactory<>(request.name, miruProvider.getStats(), "scoreAnalytics", new AnalyticsQuestion(trending,
                    request,
                    miruProvider.getRemotePartition(AnalyticsRemotePartition.class))),
                new AnalyticsAnswerEvaluator(),
                new AnalyticsAnswerMerger(keyedSegments),
                AnalyticsAnswer.EMPTY_RESULTS,
                request.logLevel);
        } catch (MiruPartitionUnavailableException | InterruptedException e) {
            throw e;
        } catch (Exception e) {
            //TODO throw http error codes
            throw new MiruQueryServiceException("Failed to score trending stream", e);
        }
    }

    public MiruPartitionResponse<AnalyticsAnswer> score(MiruPartitionId partitionId,
        MiruRequestAndReport<AnalyticsQuery, AnalyticsReport> requestAndReport) throws MiruQueryServiceException, InterruptedException {
        try {
            LOG.debug("askImmediate: partitionId={} request={}", partitionId, requestAndReport.request);
            LOG.trace("askImmediate: report={}", requestAndReport.report);
            MiruTenantId tenantId = requestAndReport.request.tenantId;
            Miru miru = miruProvider.getMiru(tenantId);
            return miru.askImmediate(tenantId,
                partitionId,
                new MiruSolvableFactory<>(requestAndReport.request.name, miruProvider.getStats(), "scoreAnalytics", new AnalyticsQuestion(trending,
                    requestAndReport.request,
                    miruProvider.getRemotePartition(AnalyticsRemotePartition.class))),
                Optional.fromNullable(requestAndReport.report),
                AnalyticsAnswer.EMPTY_RESULTS,
                requestAndReport.request.logLevel);
        } catch (MiruPartitionUnavailableException | InterruptedException e) {
            throw e;
        } catch (Exception e) {
            //TODO throw http error codes
            throw new MiruQueryServiceException("Failed to score trending stream for partition: " + partitionId.getId(), e);
        }
    }

}
