package com.jivesoftware.os.miru.stream.plugins.filter;

import com.google.common.base.Optional;
import com.jivesoftware.os.miru.api.MiruQueryServiceException;
import com.jivesoftware.os.miru.api.activity.MiruPartitionId;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.query.Miru;
import com.jivesoftware.os.miru.query.MiruPartitionUnavailableException;
import com.jivesoftware.os.miru.query.MiruProvider;
import com.jivesoftware.os.miru.query.MiruSolvableFactory;

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

    public AggregateCountsResult filterCustomStream(AggregateCountsQuery query) throws MiruQueryServiceException {
        try {
            MiruTenantId tenantId = query.tenantId;
            Miru miru = miruProvider.getMiru(tenantId);
            return miru.callAndMerge(tenantId,
                    new MiruSolvableFactory<>("filterCustomStream", new FilterCustomExecuteQuery(aggregateCounts, query)),
                    new AggregateCountsResultEvaluator(query),
                    new MergeAggregateCountResults(),
                    AggregateCountsResult.EMPTY_RESULTS);
        } catch (MiruPartitionUnavailableException e) {
            throw e;
        } catch (Exception e) {
            //TODO throw http error codes
            throw new MiruQueryServiceException("Failed to filter custom stream", e);
        }
    }

    public AggregateCountsResult filterInboxStreamAll(AggregateCountsQuery query) throws MiruQueryServiceException {
        try {
            MiruTenantId tenantId = query.tenantId;
            Miru miru = miruProvider.getMiru(tenantId);
            return miru.callAndMerge(tenantId,
                    new MiruSolvableFactory<>("filterInboxStreamAll", new FilterInboxExecuteQuery(aggregateCounts,
                            miruProvider.getBackfillerizer(tenantId), query, false)),
                    new AggregateCountsResultEvaluator(query),
                    new MergeAggregateCountResults(),
                    AggregateCountsResult.EMPTY_RESULTS);
        } catch (MiruPartitionUnavailableException e) {
            throw e;
        } catch (Exception e) {
            //TODO throw http error codes
            throw new MiruQueryServiceException("Failed to filter inbox all stream", e);
        }
    }

    public AggregateCountsResult filterInboxStreamUnread(AggregateCountsQuery query) throws MiruQueryServiceException {
        try {
            MiruTenantId tenantId = query.tenantId;
            Miru miru = miruProvider.getMiru(tenantId);
            return miru.callAndMerge(tenantId,
                    new MiruSolvableFactory<>("filterInboxStreamUnread", new FilterInboxExecuteQuery(aggregateCounts,
                            miruProvider.getBackfillerizer(tenantId), query, true)),
                    new AggregateCountsResultEvaluator(query),
                    new MergeAggregateCountResults(),
                    AggregateCountsResult.EMPTY_RESULTS);
        } catch (MiruPartitionUnavailableException e) {
            throw e;
        } catch (Exception e) {
            //TODO throw http error codes
            throw new MiruQueryServiceException("Failed to filter inbox unread stream", e);
        }
    }

    public AggregateCountsResult filterCustomStream(MiruPartitionId partitionId,
            AggregateCountsQuery query,
            Optional<AggregateCountsResult> lastResult)
            throws MiruQueryServiceException {
        try {
            MiruTenantId tenantId = query.tenantId;
            Miru miru = miruProvider.getMiru(tenantId);
            return miru.callImmediate(tenantId,
                    partitionId,
                    new MiruSolvableFactory<>("filterCustomStream", new FilterCustomExecuteQuery(aggregateCounts, query)),
                    lastResult,
                    AggregateCountsResult.EMPTY_RESULTS);
        } catch (MiruPartitionUnavailableException e) {
            throw e;
        } catch (Exception e) {
            //TODO throw http error codes
            throw new MiruQueryServiceException("Failed to filter custom stream for partition: " + partitionId.getId(), e);
        }
    }

    public AggregateCountsResult filterInboxStreamAll(MiruPartitionId partitionId,
            AggregateCountsQuery query,
            Optional<AggregateCountsResult> lastResult)
            throws MiruQueryServiceException {
        try {
            MiruTenantId tenantId = query.tenantId;
            Miru miru = miruProvider.getMiru(tenantId);
            return miru.callImmediate(tenantId,
                    partitionId,
                    new MiruSolvableFactory<>("filterInboxStreamAll", new FilterInboxExecuteQuery(aggregateCounts,
                            miruProvider.getBackfillerizer(tenantId), query, false)),
                    lastResult,
                    AggregateCountsResult.EMPTY_RESULTS);
        } catch (MiruPartitionUnavailableException e) {
            throw e;
        } catch (Exception e) {
            //TODO throw http error codes
            throw new MiruQueryServiceException("Failed to filter inbox all stream for partition: " + partitionId.getId(), e);
        }
    }

    public AggregateCountsResult filterInboxStreamUnread(MiruPartitionId partitionId,
            AggregateCountsQuery query,
            Optional<AggregateCountsResult> lastResult)
            throws MiruQueryServiceException {
        try {
            MiruTenantId tenantId = query.tenantId;
            Miru miru = miruProvider.getMiru(tenantId);
            return miru.callImmediate(tenantId,
                    partitionId,
                    new MiruSolvableFactory<>("filterInboxStreamUnread", new FilterInboxExecuteQuery(aggregateCounts,
                            miruProvider.getBackfillerizer(tenantId), query, true)),
                    lastResult,
                    AggregateCountsResult.EMPTY_RESULTS);
        } catch (MiruPartitionUnavailableException e) {
            throw e;
        } catch (Exception e) {
            //TODO throw http error codes
            throw new MiruQueryServiceException("Failed to filter inbox unread stream for partition: " + partitionId.getId(), e);
        }
    }

}
