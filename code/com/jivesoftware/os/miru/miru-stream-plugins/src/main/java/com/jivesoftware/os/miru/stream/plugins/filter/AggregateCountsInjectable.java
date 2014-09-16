package com.jivesoftware.os.miru.stream.plugins.filter;

import com.google.common.base.Optional;
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
public class AggregateCountsInjectable {

    private final MiruProvider<? extends Miru> miruProvider;
    private final AggregateCounts aggregateCounts;

    public AggregateCountsInjectable(MiruProvider<? extends Miru> miruProvider, AggregateCounts aggregateCounts) {
        this.miruProvider = miruProvider;
        this.aggregateCounts = aggregateCounts;
    }

    public MiruResponse<AggregateCountsAnswer> filterCustomStream(MiruRequest<AggregateCountsQuery> request) throws MiruQueryServiceException {
        try {
            MiruTenantId tenantId = request.tenantId;
            Miru miru = miruProvider.getMiru(tenantId);
            return miru.askAndMerge(tenantId,
                    new MiruSolvableFactory<>("filterCustomStream", new FilterCustomQuestion(aggregateCounts, request)),
                    new AggregateCountsAnswerEvaluator(request.query),
                    new AggregateCountsAnswerMerger(),
                    AggregateCountsAnswer.EMPTY_RESULTS, request.debug);
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
            Miru miru = miruProvider.getMiru(tenantId);
            return miru.askAndMerge(tenantId,
                    new MiruSolvableFactory<>("filterInboxStreamAll", new FilterInboxQuestion(aggregateCounts,
                            miruProvider.getBackfillerizer(tenantId), request, false)),
                    new AggregateCountsAnswerEvaluator(request.query),
                    new AggregateCountsAnswerMerger(),
                    AggregateCountsAnswer.EMPTY_RESULTS, request.debug);
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
            Miru miru = miruProvider.getMiru(tenantId);
            return miru.askAndMerge(tenantId,
                    new MiruSolvableFactory<>("filterInboxStreamUnread", new FilterInboxQuestion(aggregateCounts,
                            miruProvider.getBackfillerizer(tenantId), request, true)),
                    new AggregateCountsAnswerEvaluator(request.query),
                    new AggregateCountsAnswerMerger(),
                    AggregateCountsAnswer.EMPTY_RESULTS, request.debug);
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
            Miru miru = miruProvider.getMiru(tenantId);
            return miru.askImmediate(tenantId,
                    partitionId,
                    new MiruSolvableFactory<>("filterCustomStream", new FilterCustomQuestion(aggregateCounts, requestAndReport.request)),
                    Optional.fromNullable(requestAndReport.report),
                    AggregateCountsAnswer.EMPTY_RESULTS, false);
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
            Miru miru = miruProvider.getMiru(tenantId);
            return miru.askImmediate(tenantId,
                    partitionId,
                    new MiruSolvableFactory<>("filterInboxStreamAll", new FilterInboxQuestion(aggregateCounts,
                            miruProvider.getBackfillerizer(tenantId), requestAndReport.request, false)),
                    Optional.fromNullable(requestAndReport.report),
                    AggregateCountsAnswer.EMPTY_RESULTS, false);
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
            Miru miru = miruProvider.getMiru(tenantId);
            return miru.askImmediate(tenantId,
                    partitionId,
                    new MiruSolvableFactory<>("filterInboxStreamUnread", new FilterInboxQuestion(aggregateCounts,
                            miruProvider.getBackfillerizer(tenantId), requestAndReport.request, true)),
                    Optional.fromNullable(requestAndReport.report),
                    AggregateCountsAnswer.EMPTY_RESULTS, false);
        } catch (MiruPartitionUnavailableException e) {
            throw e;
        } catch (Exception e) {
            //TODO throw http error codes
            throw new MiruQueryServiceException("Failed to filter inbox unread stream for partition: " + partitionId.getId(), e);
        }
    }

}
