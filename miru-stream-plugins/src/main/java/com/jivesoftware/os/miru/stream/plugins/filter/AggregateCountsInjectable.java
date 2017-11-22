package com.jivesoftware.os.miru.stream.plugins.filter;

import com.google.common.base.Optional;
import com.jivesoftware.os.miru.api.MiruQueryServiceException;
import com.jivesoftware.os.miru.api.activity.MiruPartitionId;
import com.jivesoftware.os.miru.api.base.MiruStreamId;
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
import java.util.Set;

/**
 *
 */
public class AggregateCountsInjectable {

    private final MiruProvider<? extends Miru> provider;
    private final AggregateCounts aggregateCounts;
    private final Set<MiruStreamId> verboseStreamIds;
    private final boolean verboseAllStreamIds;

    public AggregateCountsInjectable(MiruProvider<? extends Miru> provider,
        AggregateCounts aggregateCounts,
        Set<MiruStreamId> verboseStreamIds,
        boolean verboseAllStreamIds) {
        this.provider = provider;
        this.aggregateCounts = aggregateCounts;
        this.verboseStreamIds = verboseStreamIds;
        this.verboseAllStreamIds = verboseAllStreamIds;
    }

    public MiruResponse<AggregateCountsAnswer> filterCustomStream(MiruRequest<AggregateCountsQuery> request) throws MiruQueryServiceException,
        InterruptedException {
        try {
            MiruTenantId tenantId = request.tenantId;
            Miru miru = provider.getMiru(tenantId);
            return miru.askAndMerge(tenantId,
                new MiruSolvableFactory<>(request.name,
                    provider.getStats(),
                    "filterCustomStream",
                    new AggregateCountsCustomQuestion(aggregateCounts,
                        provider.getBackfillerizer(tenantId),
                        request,
                        provider.getRemotePartition(AggregateCountsCustomRemotePartition.class),
                        verboseStreamIds,
                        verboseAllStreamIds)),
                new AggregateCountsAnswerEvaluator(request.query),
                new AggregateCountsAnswerMerger(),
                AggregateCountsAnswer.EMPTY_RESULTS,
                miru.getDefaultExecutor(),
                request.logLevel);
        } catch (MiruPartitionUnavailableException | InterruptedException e) {
            throw e;
        } catch (Exception e) {
            //TODO throw http error codes
            throw new MiruQueryServiceException("Failed to filter custom stream", e);
        }
    }

    public MiruResponse<AggregateCountsAnswer> filterInboxStream(MiruRequest<AggregateCountsQuery> request) throws MiruQueryServiceException,
        InterruptedException {
        try {
            MiruTenantId tenantId = request.tenantId;
            Miru miru = provider.getMiru(tenantId);
            return miru.askAndMerge(tenantId,
                new MiruSolvableFactory<>(request.name,
                    provider.getStats(),
                    "filterInboxStream",
                    new AggregateCountsInboxQuestion(aggregateCounts,
                        provider.getBackfillerizer(tenantId), request,
                        provider.getRemotePartition(AggregateCountsInboxAllRemotePartition.class))),
                new AggregateCountsAnswerEvaluator(request.query),
                new AggregateCountsAnswerMerger(),
                AggregateCountsAnswer.EMPTY_RESULTS,
                miru.getDefaultExecutor(),
                request.logLevel);
        } catch (MiruPartitionUnavailableException | InterruptedException e) {
            throw e;
        } catch (Exception e) {
            //TODO throw http error codes
            throw new MiruQueryServiceException("Failed to filter inbox stream", e);
        }
    }

    public MiruPartitionResponse<AggregateCountsAnswer> filterCustomStream(MiruPartitionId partitionId,
        MiruRequestAndReport<AggregateCountsQuery, AggregateCountsReport> requestAndReport)
        throws MiruQueryServiceException, InterruptedException {
        try {
            MiruTenantId tenantId = requestAndReport.request.tenantId;
            Miru miru = provider.getMiru(tenantId);
            return miru.askImmediate(tenantId,
                partitionId,
                new MiruSolvableFactory<>(requestAndReport.request.name,
                    provider.getStats(),
                    "filterCustomStream",
                    new AggregateCountsCustomQuestion(aggregateCounts,
                        provider.getBackfillerizer(tenantId),
                        requestAndReport.request,
                        provider.getRemotePartition(AggregateCountsCustomRemotePartition.class),
                        verboseStreamIds,
                        verboseAllStreamIds)),
                Optional.fromNullable(requestAndReport.report),
                AggregateCountsAnswer.EMPTY_RESULTS,
                MiruSolutionLogLevel.NONE);
        } catch (MiruPartitionUnavailableException | InterruptedException e) {
            throw e;
        } catch (Exception e) {
            //TODO throw http error codes
            throw new MiruQueryServiceException("Failed to filter custom stream for partition: " + partitionId.getId(), e);
        }
    }

    public MiruPartitionResponse<AggregateCountsAnswer> filterInboxStream(MiruPartitionId partitionId,
        MiruRequestAndReport<AggregateCountsQuery, AggregateCountsReport> requestAndReport)
        throws MiruQueryServiceException, InterruptedException {
        try {
            MiruTenantId tenantId = requestAndReport.request.tenantId;
            Miru miru = provider.getMiru(tenantId);
            return miru.askImmediate(tenantId,
                partitionId,
                new MiruSolvableFactory<>(requestAndReport.request.name,
                    provider.getStats(),
                    "filterInboxStream",
                    new AggregateCountsInboxQuestion(aggregateCounts,
                        provider.getBackfillerizer(tenantId),
                        requestAndReport.request,
                        provider.getRemotePartition(AggregateCountsInboxAllRemotePartition.class))),
                Optional.fromNullable(requestAndReport.report),
                AggregateCountsAnswer.EMPTY_RESULTS,
                MiruSolutionLogLevel.NONE);
        } catch (MiruPartitionUnavailableException | InterruptedException e) {
            throw e;
        } catch (Exception e) {
            //TODO throw http error codes
            throw new MiruQueryServiceException("Failed to filter inbox stream for partition: " + partitionId.getId(), e);
        }
    }

}
