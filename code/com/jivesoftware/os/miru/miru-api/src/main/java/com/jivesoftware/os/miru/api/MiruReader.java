package com.jivesoftware.os.miru.api;

import com.google.common.base.Optional;
import com.jivesoftware.os.miru.api.activity.MiruPartitionId;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.api.query.AggregateCountsQuery;
import com.jivesoftware.os.miru.api.query.DistinctCountQuery;
import com.jivesoftware.os.miru.api.query.RecoQuery;
import com.jivesoftware.os.miru.api.query.TrendingQuery;
import com.jivesoftware.os.miru.api.query.filter.MiruAuthzExpression;
import com.jivesoftware.os.miru.api.query.result.AggregateCountsResult;
import com.jivesoftware.os.miru.api.query.result.DistinctCountResult;
import com.jivesoftware.os.miru.api.query.result.RecoResult;
import com.jivesoftware.os.miru.api.query.result.TrendingResult;

/** This interface provides the method to read data from miru. */
public interface MiruReader {

    public static final String QUERY_SERVICE_ENDPOINT_PREFIX = "/miru/reader";

    public static final String AGGREGATE_COUNTS_INFIX = "/aggregate";
    public static final String DISTINCT_COUNT_INFIX = "/distinct";
    public static final String TRENDING_INFIX = "/trending";
    public static final String RECO_INFIX = "/reco";

    public static final String CUSTOM_QUERY_ENDPOINT = "/custom";
    public static final String INBOX_ALL_QUERY_ENDPOINT = "/inboxAll";
    public static final String INBOX_UNREAD_QUERY_ENDPOINT = "/inboxUnread";

    public static final String WARM_ENDPOINT = "/warm";

    /** Aggregate counts for a custom stream. */
    AggregateCountsResult filterCustomStream(MiruTenantId tenantId, Optional<MiruActorId> userIdentity,
        Optional<MiruAuthzExpression> authzExpression, MiruAggregateCountsQueryCriteria queryCriteria)
        throws MiruQueryServiceException;

    /** Aggregate all counts for an inbox. */
    AggregateCountsResult filterInboxStreamAll(MiruTenantId tenantId, Optional<MiruActorId> userIdentity,
        Optional<MiruAuthzExpression> authzExpression, MiruAggregateCountsQueryCriteria queryCriteria)
        throws MiruQueryServiceException;

    /** Aggregate unread counts for an inbox. */
    AggregateCountsResult filterInboxStreamUnread(MiruTenantId tenantId, Optional<MiruActorId> userIdentity,
        Optional<MiruAuthzExpression> authzExpression, MiruAggregateCountsQueryCriteria queryCriteria)
        throws MiruQueryServiceException;

    /** Aggregate counts for a custom stream on a specific local partition. */
    AggregateCountsResult filterCustomStream(MiruPartitionId partitionId, AggregateCountsQuery query, Optional<AggregateCountsResult> lastResult)
        throws MiruQueryServiceException;

    /** Aggregate all counts for an inbox on a specific local partition. */
    AggregateCountsResult filterInboxStreamAll(MiruPartitionId partitionId, AggregateCountsQuery query, Optional<AggregateCountsResult> lastResult)
        throws MiruQueryServiceException;

    /** Aggregate unread counts for an inbox on a specific local partition. */
    AggregateCountsResult filterInboxStreamUnread(MiruPartitionId partitionId, AggregateCountsQuery query, Optional<AggregateCountsResult> lastResult)
        throws MiruQueryServiceException;

    /** Count distincts for a custom stream. */
    DistinctCountResult countCustomStream(MiruTenantId tenantId, Optional<MiruActorId> userIdentity,
        Optional<MiruAuthzExpression> authzExpression, MiruDistinctCountQueryCriteria queryCriteria)
        throws MiruQueryServiceException;

    /** Count all distincts for an inbox. */
    DistinctCountResult countInboxStreamAll(MiruTenantId tenantId, Optional<MiruActorId> userIdentity,
        Optional<MiruAuthzExpression> authzExpression, MiruDistinctCountQueryCriteria queryCriteria)
        throws MiruQueryServiceException;

    /** Count unread distincts for an inbox. */
    DistinctCountResult countInboxStreamUnread(MiruTenantId tenantId, Optional<MiruActorId> userIdentity,
        Optional<MiruAuthzExpression> authzExpression, MiruDistinctCountQueryCriteria queryCriteria)
        throws MiruQueryServiceException;

    /** Count distincts for a custom stream on a specific local partition. */
    DistinctCountResult countCustomStream(MiruPartitionId partitionId, DistinctCountQuery query, Optional<DistinctCountResult> lastResult)
        throws MiruQueryServiceException;

    /** Count distincts for a custom stream on a specific local partition. */
    DistinctCountResult countInboxStreamAll(MiruPartitionId partitionId, DistinctCountQuery query, Optional<DistinctCountResult> lastResult)
        throws MiruQueryServiceException;

    /** Count distincts for a custom stream on a specific local partition. */
    DistinctCountResult countInboxStreamUnread(MiruPartitionId partitionId, DistinctCountQuery query, Optional<DistinctCountResult> lastResult)
        throws MiruQueryServiceException;

    /** Score trending. */
    TrendingResult scoreTrending(MiruTenantId tenantId, Optional<MiruActorId> userIdentity, Optional<MiruAuthzExpression> authzExpression,
        MiruTrendingQueryCriteria queryCriteria) throws MiruQueryServiceException;

    /** Score trending on a specific local partition. */
    TrendingResult scoreTrending(MiruPartitionId partitionId, TrendingQuery query, Optional<TrendingResult> lastResult) throws MiruQueryServiceException;


    RecoResult collaborativeFilteringRecommendations(MiruTenantId tenantId, Optional<MiruActorId> userIdentity, Optional<MiruAuthzExpression> authzExpression,
        MiruRecoQueryCriteria queryCriteria) throws MiruQueryServiceException;

    RecoResult collaborativeFilteringRecommendations(MiruPartitionId partitionId, RecoQuery query, Optional<RecoResult> lastResult)
            throws MiruQueryServiceException;

    /** Warm up tenant. */
    void warm(MiruTenantId tenantId) throws MiruQueryServiceException;
}