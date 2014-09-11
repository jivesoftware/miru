package com.jivesoftware.os.miru.stream.plugins.filter;

import com.google.common.base.Optional;
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
public class AggregateCountsInjectable {

    private final MiruProvider<? extends Miru> miruProvider;
    private final AggregateCounts aggregateCounts;

    public AggregateCountsInjectable(MiruProvider<? extends Miru> miruProvider, AggregateCounts aggregateCounts) {
        this.miruProvider = miruProvider;
        this.aggregateCounts = aggregateCounts;
    }

    public MiruResponse<AggregateCountsAnswer> filterCustomStream(AggregateCountsQuery query) throws MiruQueryServiceException {
        try {
            MiruTenantId tenantId = query.tenantId;
            Miru miru = miruProvider.getMiru(tenantId);
            return miru.askAndMerge(tenantId,
                    new MiruSolvableFactory<>("filterCustomStream", new FilterCustomQuestion(aggregateCounts, query)),
                    new AggregateCountsAnswerEvaluator(query),
                    new AggregateCountsAnswerMerger(),
                    AggregateCountsAnswer.EMPTY_RESULTS);
        } catch (MiruPartitionUnavailableException e) {
            throw e;
        } catch (Exception e) {
            //TODO throw http error codes
            throw new MiruQueryServiceException("Failed to filter custom stream", e);
        }
    }

    public MiruResponse<AggregateCountsAnswer> filterInboxStreamAll(AggregateCountsQuery query) throws MiruQueryServiceException {
        try {
            MiruTenantId tenantId = query.tenantId;
            Miru miru = miruProvider.getMiru(tenantId);
            return miru.askAndMerge(tenantId,
                    new MiruSolvableFactory<>("filterInboxStreamAll", new FilterInboxQuestion(aggregateCounts,
                            miruProvider.getBackfillerizer(tenantId), query, false)),
                    new AggregateCountsAnswerEvaluator(query),
                    new AggregateCountsAnswerMerger(),
                    AggregateCountsAnswer.EMPTY_RESULTS);
        } catch (MiruPartitionUnavailableException e) {
            throw e;
        } catch (Exception e) {
            //TODO throw http error codes
            throw new MiruQueryServiceException("Failed to filter inbox all stream", e);
        }
    }

    public MiruResponse<AggregateCountsAnswer> filterInboxStreamUnread(AggregateCountsQuery query) throws MiruQueryServiceException {
        try {
            MiruTenantId tenantId = query.tenantId;
            Miru miru = miruProvider.getMiru(tenantId);
            return miru.askAndMerge(tenantId,
                    new MiruSolvableFactory<>("filterInboxStreamUnread", new FilterInboxQuestion(aggregateCounts,
                            miruProvider.getBackfillerizer(tenantId), query, true)),
                    new AggregateCountsAnswerEvaluator(query),
                    new AggregateCountsAnswerMerger(),
                    AggregateCountsAnswer.EMPTY_RESULTS);
        } catch (MiruPartitionUnavailableException e) {
            throw e;
        } catch (Exception e) {
            //TODO throw http error codes
            throw new MiruQueryServiceException("Failed to filter inbox unread stream", e);
        }
    }

    public AggregateCountsAnswer filterCustomStream(MiruPartitionId partitionId,
            AggregateCountsQueryAndReport queryAndReport)
            throws MiruQueryServiceException {
        try {
            MiruTenantId tenantId = queryAndReport.query.tenantId;
            Miru miru = miruProvider.getMiru(tenantId);
            return miru.askImmediate(tenantId,
                    partitionId,
                    new MiruSolvableFactory<>("filterCustomStream", new FilterCustomQuestion(aggregateCounts, queryAndReport.query)),
                    Optional.fromNullable(queryAndReport.report),
                    AggregateCountsAnswer.EMPTY_RESULTS);
        } catch (MiruPartitionUnavailableException e) {
            throw e;
        } catch (Exception e) {
            //TODO throw http error codes
            throw new MiruQueryServiceException("Failed to filter custom stream for partition: " + partitionId.getId(), e);
        }
    }

    public AggregateCountsAnswer filterInboxStreamAll(MiruPartitionId partitionId,
            AggregateCountsQueryAndReport queryAndReport)
            throws MiruQueryServiceException {
        try {
            MiruTenantId tenantId = queryAndReport.query.tenantId;
            Miru miru = miruProvider.getMiru(tenantId);
            return miru.askImmediate(tenantId,
                    partitionId,
                    new MiruSolvableFactory<>("filterInboxStreamAll", new FilterInboxQuestion(aggregateCounts,
                            miruProvider.getBackfillerizer(tenantId), queryAndReport.query, false)),
                    Optional.fromNullable(queryAndReport.report),
                    AggregateCountsAnswer.EMPTY_RESULTS);
        } catch (MiruPartitionUnavailableException e) {
            throw e;
        } catch (Exception e) {
            //TODO throw http error codes
            throw new MiruQueryServiceException("Failed to filter inbox all stream for partition: " + partitionId.getId(), e);
        }
    }

    public AggregateCountsAnswer filterInboxStreamUnread(MiruPartitionId partitionId,
            AggregateCountsQueryAndReport queryAndReport)
            throws MiruQueryServiceException {
        try {
            MiruTenantId tenantId = queryAndReport.query.tenantId;
            Miru miru = miruProvider.getMiru(tenantId);
            return miru.askImmediate(tenantId,
                    partitionId,
                    new MiruSolvableFactory<>("filterInboxStreamUnread", new FilterInboxQuestion(aggregateCounts,
                            miruProvider.getBackfillerizer(tenantId), queryAndReport.query, true)),
                    Optional.fromNullable(queryAndReport.report),
                    AggregateCountsAnswer.EMPTY_RESULTS);
        } catch (MiruPartitionUnavailableException e) {
            throw e;
        } catch (Exception e) {
            //TODO throw http error codes
            throw new MiruQueryServiceException("Failed to filter inbox unread stream for partition: " + partitionId.getId(), e);
        }
    }

}
