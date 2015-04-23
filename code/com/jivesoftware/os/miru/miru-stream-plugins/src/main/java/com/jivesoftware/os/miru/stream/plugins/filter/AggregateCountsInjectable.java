package com.jivesoftware.os.miru.stream.plugins.filter;

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

/**
 *
 */
public class AggregateCountsInjectable {

    private final MiruProvider<? extends Miru> provider;
    private final AggregateCounts aggregateCounts;

    public AggregateCountsInjectable(MiruProvider<? extends Miru> provider, AggregateCounts aggregateCounts) {
        this.provider = provider;
        this.aggregateCounts = aggregateCounts;
    }

    public MiruResponse<AggregateCountsAnswer> filterCustomStream(MiruRequest<AggregateCountsQuery> request) throws MiruQueryServiceException {
        try {
            MiruTenantId tenantId = request.tenantId;
            Miru miru = provider.getMiru(tenantId);
            return miru.askAndMerge(tenantId,
                new MiruSolvableFactory<>(provider.getStats(), "filterCustomStream", new AggregateCountsCustomQuestion(aggregateCounts, request)),
                new AggregateCountsAnswerEvaluator(request.query),
                new AggregateCountsAnswerMerger(),
                AggregateCountsAnswer.EMPTY_RESULTS,
                request.logLevel);
        } catch (MiruPartitionUnavailableException e) {
            throw e;
        } catch (Exception e) {
            //TODO throw http error codes
            throw new MiruQueryServiceException("Failed to filter custom stream", e);
        }
    }

    public MiruResponse<AggregateCountsAnswer> filterInboxStreamAll(MiruRequest<AggregateCountsQuery> request) throws MiruQueryServiceException {
        try {
            MiruTenantId tenantId = request.tenantId;
            Miru miru = provider.getMiru(tenantId);
            return miru.askAndMerge(tenantId,
                new MiruSolvableFactory<>(provider.getStats(), "filterInboxStreamAll", new AggregateCountsInboxQuestion(aggregateCounts,
                    provider.getBackfillerizer(tenantId), request, false)),
                new AggregateCountsAnswerEvaluator(request.query),
                new AggregateCountsAnswerMerger(),
                AggregateCountsAnswer.EMPTY_RESULTS,
                request.logLevel);
        } catch (MiruPartitionUnavailableException e) {
            throw e;
        } catch (Exception e) {
            //TODO throw http error codes
            throw new MiruQueryServiceException("Failed to filter inbox all stream", e);
        }
    }

    public MiruResponse<AggregateCountsAnswer> filterInboxStreamUnread(MiruRequest<AggregateCountsQuery> request) throws MiruQueryServiceException {
        try {
            MiruTenantId tenantId = request.tenantId;
            Miru miru = provider.getMiru(tenantId);
            return miru.askAndMerge(tenantId,
                new MiruSolvableFactory<>(provider.getStats(), "filterInboxStreamUnread", new AggregateCountsInboxQuestion(aggregateCounts,
                    provider.getBackfillerizer(tenantId), request, true)),
                new AggregateCountsAnswerEvaluator(request.query),
                new AggregateCountsAnswerMerger(),
                AggregateCountsAnswer.EMPTY_RESULTS,
                request.logLevel);
        } catch (MiruPartitionUnavailableException e) {
            throw e;
        } catch (Exception e) {
            //TODO throw http error codes
            throw new MiruQueryServiceException("Failed to filter inbox unread stream", e);
        }
    }

    public MiruPartitionResponse<AggregateCountsAnswer> filterCustomStream(MiruPartitionId partitionId,
        MiruRequestAndReport<AggregateCountsQuery, AggregateCountsReport> requestAndReport)
        throws MiruQueryServiceException {
        try {
            MiruTenantId tenantId = requestAndReport.request.tenantId;
            Miru miru = provider.getMiru(tenantId);
            return miru.askImmediate(tenantId,
                partitionId,
                new MiruSolvableFactory<>(provider.getStats(), "filterCustomStream", new AggregateCountsCustomQuestion(aggregateCounts, requestAndReport.request)),
                Optional.fromNullable(requestAndReport.report),
                AggregateCountsAnswer.EMPTY_RESULTS,
                MiruSolutionLogLevel.NONE);
        } catch (MiruPartitionUnavailableException e) {
            throw e;
        } catch (Exception e) {
            //TODO throw http error codes
            throw new MiruQueryServiceException("Failed to filter custom stream for partition: " + partitionId.getId(), e);
        }
    }

    public MiruPartitionResponse<AggregateCountsAnswer> filterInboxStreamAll(MiruPartitionId partitionId,
        MiruRequestAndReport<AggregateCountsQuery, AggregateCountsReport> requestAndReport)
        throws MiruQueryServiceException {
        try {
            MiruTenantId tenantId = requestAndReport.request.tenantId;
            Miru miru = provider.getMiru(tenantId);
            return miru.askImmediate(tenantId,
                partitionId,
                new MiruSolvableFactory<>(provider.getStats(), "filterInboxStreamAll", new AggregateCountsInboxQuestion(aggregateCounts,
                    provider.getBackfillerizer(tenantId), requestAndReport.request, false)),
                Optional.fromNullable(requestAndReport.report),
                AggregateCountsAnswer.EMPTY_RESULTS,
                MiruSolutionLogLevel.NONE);
        } catch (MiruPartitionUnavailableException e) {
            throw e;
        } catch (Exception e) {
            //TODO throw http error codes
            throw new MiruQueryServiceException("Failed to filter inbox all stream for partition: " + partitionId.getId(), e);
        }
    }

    public MiruPartitionResponse<AggregateCountsAnswer> filterInboxStreamUnread(MiruPartitionId partitionId,
        MiruRequestAndReport<AggregateCountsQuery, AggregateCountsReport> requestAndReport)
        throws MiruQueryServiceException {
        try {
            MiruTenantId tenantId = requestAndReport.request.tenantId;
            Miru miru = provider.getMiru(tenantId);
            return miru.askImmediate(tenantId,
                partitionId,
                new MiruSolvableFactory<>(provider.getStats(), "filterInboxStreamUnread", new AggregateCountsInboxQuestion(aggregateCounts,
                    provider.getBackfillerizer(tenantId), requestAndReport.request, true)),
                Optional.fromNullable(requestAndReport.report),
                AggregateCountsAnswer.EMPTY_RESULTS,
                MiruSolutionLogLevel.NONE);
        } catch (MiruPartitionUnavailableException e) {
            throw e;
        } catch (Exception e) {
            //TODO throw http error codes
            throw new MiruQueryServiceException("Failed to filter inbox unread stream for partition: " + partitionId.getId(), e);
        }
    }

}
