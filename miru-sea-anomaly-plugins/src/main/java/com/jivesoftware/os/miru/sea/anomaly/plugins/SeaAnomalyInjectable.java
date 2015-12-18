package com.jivesoftware.os.miru.sea.anomaly.plugins;

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
public class SeaAnomalyInjectable {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    private final MiruProvider<? extends Miru> provider;
    private final SeaAnomaly seaAnomaly;

    public SeaAnomalyInjectable(MiruProvider<? extends Miru> provider, SeaAnomaly seaAnomaly) {
        this.provider = provider;
        this.seaAnomaly = seaAnomaly;
    }

    public MiruResponse<SeaAnomalyAnswer> score(MiruRequest<SeaAnomalyQuery> request) throws MiruQueryServiceException, InterruptedException {
        try {
            LOG.debug("askAndMerge: request={}", request);
            MiruTenantId tenantId = request.tenantId;
            Miru miru = provider.getMiru(tenantId);
            return miru.askAndMerge(tenantId,
                new MiruSolvableFactory<>(request.name, provider.getStats(), "scoreAnomaly", new SeaAnomalyQuestion(seaAnomaly,
                    request,
                    provider.getRemotePartition(SeaAnomalyRemotePartition.class))),
                new SeaAnomalyAnswerEvaluator(),
                new SeaAnomalyAnswerMerger(),
                SeaAnomalyAnswer.EMPTY_RESULTS,
                request.logLevel);
        } catch (MiruPartitionUnavailableException | InterruptedException e) {
            throw e;
        } catch (Exception e) {
            //TODO throw http error codes
            throw new MiruQueryServiceException("Failed to score trending stream", e);
        }
    }

    public MiruPartitionResponse<SeaAnomalyAnswer> score(MiruPartitionId partitionId,
        MiruRequestAndReport<SeaAnomalyQuery, SeaAnomalyReport> requestAndReport) throws MiruQueryServiceException, InterruptedException {
        try {
            LOG.debug("askImmediate: partitionId={} request={}", partitionId, requestAndReport.request);
            LOG.trace("askImmediate: report={}", requestAndReport.report);
            MiruTenantId tenantId = requestAndReport.request.tenantId;
            Miru miru = provider.getMiru(tenantId);
            return miru.askImmediate(tenantId,
                partitionId,
                new MiruSolvableFactory<>(requestAndReport.request.name, provider.getStats(), "scoreTrending", new SeaAnomalyQuestion(seaAnomaly,
                    requestAndReport.request,
                    provider.getRemotePartition(SeaAnomalyRemotePartition.class))),
                Optional.fromNullable(requestAndReport.report),
                SeaAnomalyAnswer.EMPTY_RESULTS,
                requestAndReport.request.logLevel);
        } catch (MiruPartitionUnavailableException | InterruptedException e) {
            throw e;
        } catch (Exception e) {
            //TODO throw http error codes
            throw new MiruQueryServiceException("Failed to score trending stream for partition: " + partitionId.getId(), e);
        }
    }

}
