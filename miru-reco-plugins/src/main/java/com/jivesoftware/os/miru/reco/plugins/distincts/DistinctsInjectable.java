package com.jivesoftware.os.miru.reco.plugins.distincts;

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
import com.jivesoftware.os.miru.plugin.solution.MiruSolutionLogLevel;
import com.jivesoftware.os.miru.plugin.solution.MiruSolvableFactory;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;

/**
 *
 */
public class DistinctsInjectable {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    private final MiruProvider<? extends Miru> provider;
    private final Distincts distincts;

    public DistinctsInjectable(MiruProvider<? extends Miru> provider,
        Distincts distincts) {
        this.provider = provider;
        this.distincts = distincts;
    }

    public MiruResponse<DistinctsAnswer> gatherDistincts(MiruRequest<DistinctsQuery> request) throws MiruQueryServiceException, InterruptedException {
        try {
            LOG.debug("gatherDistincts: request={}", request);

            MiruTenantId tenantId = request.tenantId;
            Miru miru = provider.getMiru(tenantId);
            return miru.askAndMerge(
                tenantId,
                new MiruSolvableFactory<>(
                    request.name,
                    provider.getStats(),
                    "gatherDistincts",
                    new DistinctsQuestion(
                        distincts,
                        request,
                        provider.getRemotePartition(DistinctsRemotePartition.class))),
                new DistinctsAnswerEvaluator(),
                new DistinctsAnswerMerger(),
                DistinctsAnswer.EMPTY_RESULTS,
                miru.getDefaultExecutor(),
                request.logLevel);
        } catch (MiruPartitionUnavailableException | InterruptedException e) {
            throw e;
        } catch (Exception e) {
            //TODO throw http error codes
            throw new MiruQueryServiceException("Failed to gather distincts", e);
        }
    }

    public MiruPartitionResponse<DistinctsAnswer> gatherDistincts(MiruPartitionId partitionId,
        MiruRequestAndReport<DistinctsQuery, DistinctsReport> requestAndReport)
        throws MiruQueryServiceException, InterruptedException {
        try {
            LOG.debug("askImmediate: partitionId={} request={}", partitionId, requestAndReport.request);
            LOG.trace("askImmediate: report={}", requestAndReport.report);

            MiruTenantId tenantId = requestAndReport.request.tenantId;
            Miru miru = provider.getMiru(tenantId);
            return miru.askImmediate(
                tenantId,
                partitionId,
                new MiruSolvableFactory<>(
                    requestAndReport.request.name,
                    provider.getStats(),
                    "gatherDistincts",
                    new DistinctsQuestion(
                        distincts,
                        requestAndReport.request,
                        provider.getRemotePartition(DistinctsRemotePartition.class))),
                Optional.fromNullable(requestAndReport.report),
                DistinctsAnswer.EMPTY_RESULTS,
                MiruSolutionLogLevel.NONE);
        } catch (MiruPartitionUnavailableException | InterruptedException e) {
            throw e;
        } catch (Exception e) {
            //TODO throw http error codes
            throw new MiruQueryServiceException("Failed to gather distincts for partition: " + partitionId.getId(), e);
        }
    }

}
