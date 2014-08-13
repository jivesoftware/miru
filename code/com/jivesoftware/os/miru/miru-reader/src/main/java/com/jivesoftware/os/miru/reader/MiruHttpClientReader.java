package com.jivesoftware.os.miru.reader;

import com.google.common.base.Optional;
import com.jivesoftware.os.jive.utils.http.client.rest.RequestHelper;
import com.jivesoftware.os.jive.utils.logger.EndPointMetrics;
import com.jivesoftware.os.jive.utils.logger.MetricLogger;
import com.jivesoftware.os.jive.utils.logger.MetricLoggerFactory;
import com.jivesoftware.os.miru.api.MiruActorId;
import com.jivesoftware.os.miru.api.MiruAggregateCountsQueryAndResultParams;
import com.jivesoftware.os.miru.api.MiruAggregateCountsQueryCriteria;
import com.jivesoftware.os.miru.api.MiruAggregateCountsQueryParams;
import com.jivesoftware.os.miru.api.MiruDistinctCountQueryAndResultParams;
import com.jivesoftware.os.miru.api.MiruDistinctCountQueryCriteria;
import com.jivesoftware.os.miru.api.MiruDistinctCountQueryParams;
import com.jivesoftware.os.miru.api.MiruQueryServiceException;
import com.jivesoftware.os.miru.api.MiruReader;
import com.jivesoftware.os.miru.api.MiruRecoQueryAndResultParams;
import com.jivesoftware.os.miru.api.MiruRecoQueryCriteria;
import com.jivesoftware.os.miru.api.MiruRecoQueryParams;
import com.jivesoftware.os.miru.api.MiruTrendingQueryAndResultParams;
import com.jivesoftware.os.miru.api.MiruTrendingQueryCriteria;
import com.jivesoftware.os.miru.api.MiruTrendingQueryParams;
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

/**
 *
 */
public class MiruHttpClientReader implements MiruReader {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    private final EndPointMetrics processMetrics;
    private final RequestHelper requestHelper;

    public MiruHttpClientReader(RequestHelper requestHelper) {
        this.requestHelper = requestHelper;
        this.processMetrics = new EndPointMetrics("process", LOG);
    }

    @Override
    public AggregateCountsResult filterCustomStream(MiruTenantId tenantId, Optional<MiruActorId> userIdentity, Optional<MiruAuthzExpression> authzExpression,
        MiruAggregateCountsQueryCriteria queryCriteria) throws MiruQueryServiceException {

        MiruAggregateCountsQueryParams params = new MiruAggregateCountsQueryParams(tenantId, userIdentity, authzExpression, queryCriteria);
        processMetrics.start();
        try {
            return requestHelper.executeRequest(params,
                QUERY_SERVICE_ENDPOINT_PREFIX + AGGREGATE_COUNTS_INFIX + CUSTOM_QUERY_ENDPOINT,
                AggregateCountsResult.class, AggregateCountsResult.EMPTY_RESULTS);
        } catch (RuntimeException e) {
            throw new MiruQueryServiceException("Failed filter custom stream", e);
        } finally {
            processMetrics.stop();
        }
    }

    @Override
    public AggregateCountsResult filterInboxStreamAll(MiruTenantId tenantId, Optional<MiruActorId> userIdentity, Optional<MiruAuthzExpression> authzExpression,
        MiruAggregateCountsQueryCriteria queryCriteria) throws MiruQueryServiceException {

        MiruAggregateCountsQueryParams params = new MiruAggregateCountsQueryParams(tenantId, userIdentity, authzExpression, queryCriteria);
        processMetrics.start();
        try {
            return requestHelper.executeRequest(params,
                QUERY_SERVICE_ENDPOINT_PREFIX + AGGREGATE_COUNTS_INFIX + INBOX_ALL_QUERY_ENDPOINT,
                AggregateCountsResult.class, AggregateCountsResult.EMPTY_RESULTS);
        } catch (RuntimeException e) {
            throw new MiruQueryServiceException("Failed filter inbox all stream", e);
        } finally {
            processMetrics.stop();
        }
    }

    @Override
    public AggregateCountsResult filterInboxStreamUnread(MiruTenantId tenantId, Optional<MiruActorId> userIdentity,
        Optional<MiruAuthzExpression> authzExpression, MiruAggregateCountsQueryCriteria queryCriteria) throws MiruQueryServiceException {

        MiruAggregateCountsQueryParams params = new MiruAggregateCountsQueryParams(tenantId, userIdentity, authzExpression, queryCriteria);
        processMetrics.start();
        try {
            return requestHelper.executeRequest(params,
                QUERY_SERVICE_ENDPOINT_PREFIX + AGGREGATE_COUNTS_INFIX + INBOX_UNREAD_QUERY_ENDPOINT,
                AggregateCountsResult.class, AggregateCountsResult.EMPTY_RESULTS);
        } catch (RuntimeException e) {
            throw new MiruQueryServiceException("Failed filter inbox unread stream", e);
        } finally {
            processMetrics.stop();
        }
    }

    @Override
    public AggregateCountsResult filterCustomStream(MiruPartitionId partitionId, AggregateCountsQuery query, Optional<AggregateCountsResult> lastResult)
        throws MiruQueryServiceException {

        MiruAggregateCountsQueryAndResultParams params = new MiruAggregateCountsQueryAndResultParams(query, lastResult.orNull());
        processMetrics.start();
        try {
            return requestHelper.executeRequest(params,
                QUERY_SERVICE_ENDPOINT_PREFIX + AGGREGATE_COUNTS_INFIX + CUSTOM_QUERY_ENDPOINT + "/" + partitionId.getId(),
                AggregateCountsResult.class, AggregateCountsResult.EMPTY_RESULTS);
        } catch (RuntimeException e) {
            throw new MiruQueryServiceException("Failed filter custom stream for partition: " + partitionId.getId(), e);
        } finally {
            processMetrics.stop();
        }
    }

    @Override
    public AggregateCountsResult filterInboxStreamAll(MiruPartitionId partitionId, AggregateCountsQuery query, Optional<AggregateCountsResult> lastResult)
        throws MiruQueryServiceException {

        MiruAggregateCountsQueryAndResultParams params = new MiruAggregateCountsQueryAndResultParams(query, lastResult.orNull());
        processMetrics.start();
        try {
            return requestHelper.executeRequest(params,
                QUERY_SERVICE_ENDPOINT_PREFIX + AGGREGATE_COUNTS_INFIX + INBOX_ALL_QUERY_ENDPOINT + "/" + partitionId.getId(),
                AggregateCountsResult.class, AggregateCountsResult.EMPTY_RESULTS);
        } catch (RuntimeException e) {
            throw new MiruQueryServiceException("Failed filter inbox all stream for partition: " + partitionId.getId(), e);
        } finally {
            processMetrics.stop();
        }
    }

    @Override
    public AggregateCountsResult filterInboxStreamUnread(MiruPartitionId partitionId, AggregateCountsQuery query, Optional<AggregateCountsResult> lastResult)
        throws MiruQueryServiceException {

        MiruAggregateCountsQueryAndResultParams params = new MiruAggregateCountsQueryAndResultParams(query, lastResult.orNull());
        processMetrics.start();
        try {
            return requestHelper.executeRequest(params,
                QUERY_SERVICE_ENDPOINT_PREFIX + AGGREGATE_COUNTS_INFIX + INBOX_UNREAD_QUERY_ENDPOINT + "/" + partitionId.getId(),
                AggregateCountsResult.class, AggregateCountsResult.EMPTY_RESULTS);
        } catch (RuntimeException e) {
            throw new MiruQueryServiceException("Failed filter inbox unread stream for partition: " + partitionId.getId(), e);
        } finally {
            processMetrics.stop();
        }
    }

    @Override
    public DistinctCountResult countCustomStream(MiruTenantId tenantId, Optional<MiruActorId> userIdentity, Optional<MiruAuthzExpression> authzExpression,
        MiruDistinctCountQueryCriteria queryCriteria) throws MiruQueryServiceException {

        MiruDistinctCountQueryParams params = new MiruDistinctCountQueryParams(tenantId, userIdentity, authzExpression, queryCriteria);
        processMetrics.start();
        try {
            return requestHelper.executeRequest(params,
                QUERY_SERVICE_ENDPOINT_PREFIX + DISTINCT_COUNT_INFIX + CUSTOM_QUERY_ENDPOINT,
                DistinctCountResult.class, DistinctCountResult.EMPTY_RESULTS);
        } catch (RuntimeException e) {
            throw new MiruQueryServiceException("Failed count custom stream", e);
        } finally {
            processMetrics.stop();
        }
    }

    @Override
    public DistinctCountResult countInboxStreamAll(MiruTenantId tenantId, Optional<MiruActorId> userIdentity, Optional<MiruAuthzExpression> authzExpression,
        MiruDistinctCountQueryCriteria queryCriteria) throws MiruQueryServiceException {

        MiruDistinctCountQueryParams params = new MiruDistinctCountQueryParams(tenantId, userIdentity, authzExpression, queryCriteria);
        processMetrics.start();
        try {
            return requestHelper.executeRequest(params,
                QUERY_SERVICE_ENDPOINT_PREFIX + DISTINCT_COUNT_INFIX + INBOX_ALL_QUERY_ENDPOINT,
                DistinctCountResult.class, DistinctCountResult.EMPTY_RESULTS);
        } catch (RuntimeException e) {
            throw new MiruQueryServiceException("Failed count inbox all stream", e);
        } finally {
            processMetrics.stop();
        }
    }

    @Override
    public DistinctCountResult countInboxStreamUnread(MiruTenantId tenantId, Optional<MiruActorId> userIdentity, Optional<MiruAuthzExpression> authzExpression,
        MiruDistinctCountQueryCriteria queryCriteria) throws MiruQueryServiceException {

        MiruDistinctCountQueryParams params = new MiruDistinctCountQueryParams(tenantId, userIdentity, authzExpression, queryCriteria);
        processMetrics.start();
        try {
            return requestHelper.executeRequest(params,
                QUERY_SERVICE_ENDPOINT_PREFIX + DISTINCT_COUNT_INFIX + INBOX_UNREAD_QUERY_ENDPOINT,
                DistinctCountResult.class, DistinctCountResult.EMPTY_RESULTS);
        } catch (RuntimeException e) {
            throw new MiruQueryServiceException("Failed count inbox unread stream", e);
        } finally {
            processMetrics.stop();
        }
    }

    @Override
    public void warm(MiruTenantId tenantId) throws MiruQueryServiceException {
        processMetrics.start();
        try {
            requestHelper.executeRequest(tenantId,
                QUERY_SERVICE_ENDPOINT_PREFIX + WARM_ENDPOINT,
                String.class, "");
        } catch (RuntimeException e) {
            throw new MiruQueryServiceException("Failed count inbox unread stream", e);
        } finally {
            processMetrics.stop();
        }
    }

    @Override
    public DistinctCountResult countCustomStream(MiruPartitionId partitionId, DistinctCountQuery query, Optional<DistinctCountResult> lastResult)
        throws MiruQueryServiceException {

        MiruDistinctCountQueryAndResultParams params = new MiruDistinctCountQueryAndResultParams(query, lastResult.orNull());
        processMetrics.start();
        try {
            return requestHelper.executeRequest(params,
                QUERY_SERVICE_ENDPOINT_PREFIX + DISTINCT_COUNT_INFIX + CUSTOM_QUERY_ENDPOINT + "/" + partitionId.getId(),
                DistinctCountResult.class, DistinctCountResult.EMPTY_RESULTS);
        } catch (RuntimeException e) {
            throw new MiruQueryServiceException("Failed count custom stream for partition: " + partitionId.getId(), e);
        } finally {
            processMetrics.stop();
        }
    }

    @Override
    public DistinctCountResult countInboxStreamAll(MiruPartitionId partitionId, DistinctCountQuery query, Optional<DistinctCountResult> lastResult)
        throws MiruQueryServiceException {

        MiruDistinctCountQueryAndResultParams params = new MiruDistinctCountQueryAndResultParams(query, lastResult.orNull());
        processMetrics.start();
        try {
            return requestHelper.executeRequest(params,
                QUERY_SERVICE_ENDPOINT_PREFIX + DISTINCT_COUNT_INFIX + INBOX_ALL_QUERY_ENDPOINT + "/" + partitionId.getId(),
                DistinctCountResult.class, DistinctCountResult.EMPTY_RESULTS);
        } catch (RuntimeException e) {
            throw new MiruQueryServiceException("Failed count inbox all stream for partition: " + partitionId.getId(), e);
        } finally {
            processMetrics.stop();
        }
    }

    @Override
    public DistinctCountResult countInboxStreamUnread(MiruPartitionId partitionId, DistinctCountQuery query, Optional<DistinctCountResult> lastResult)
        throws MiruQueryServiceException {

        MiruDistinctCountQueryAndResultParams params = new MiruDistinctCountQueryAndResultParams(query, lastResult.orNull());
        processMetrics.start();
        try {
            return requestHelper.executeRequest(params,
                QUERY_SERVICE_ENDPOINT_PREFIX + DISTINCT_COUNT_INFIX + INBOX_UNREAD_QUERY_ENDPOINT + "/" + partitionId.getId(),
                DistinctCountResult.class, DistinctCountResult.EMPTY_RESULTS);
        } catch (RuntimeException e) {
            throw new MiruQueryServiceException("Failed count inbox unread stream for partition: " + partitionId.getId(), e);
        } finally {
            processMetrics.stop();
        }
    }

    @Override
    public TrendingResult scoreTrending(MiruTenantId tenantId, Optional<MiruActorId> userIdentity, Optional<MiruAuthzExpression> authzExpression,
        MiruTrendingQueryCriteria queryCriteria) throws MiruQueryServiceException {

        MiruTrendingQueryParams params = new MiruTrendingQueryParams(tenantId, userIdentity, authzExpression, queryCriteria);
        processMetrics.start();
        try {
            return requestHelper.executeRequest(params,
                QUERY_SERVICE_ENDPOINT_PREFIX + TRENDING_INFIX + CUSTOM_QUERY_ENDPOINT,
                TrendingResult.class, TrendingResult.EMPTY_RESULTS);
        } catch (RuntimeException e) {
            throw new MiruQueryServiceException("Failed score trending stream", e);
        } finally {
            processMetrics.stop();
        }
    }

    @Override
    public TrendingResult scoreTrending(MiruPartitionId partitionId, TrendingQuery query, Optional<TrendingResult> lastResult)
        throws MiruQueryServiceException {

        MiruTrendingQueryAndResultParams params = new MiruTrendingQueryAndResultParams(query, lastResult.orNull());
        processMetrics.start();
        try {
            return requestHelper.executeRequest(params,
                QUERY_SERVICE_ENDPOINT_PREFIX + TRENDING_INFIX + CUSTOM_QUERY_ENDPOINT + "/" + partitionId.getId(),
                TrendingResult.class, TrendingResult.EMPTY_RESULTS);
        } catch (RuntimeException e) {
            throw new MiruQueryServiceException("Failed score trending stream for partition: " + partitionId.getId(), e);
        } finally {
            processMetrics.stop();
        }
    }

    @Override
    public RecoResult collaborativeFilteringRecommendations(MiruTenantId tenantId, Optional<MiruActorId> userIdentity, Optional<MiruAuthzExpression> authzExpression, MiruRecoQueryCriteria queryCriteria) throws MiruQueryServiceException {
        MiruRecoQueryParams params = new MiruRecoQueryParams(tenantId, userIdentity, authzExpression, queryCriteria);
        processMetrics.start();
        try {
            return requestHelper.executeRequest(params,
                QUERY_SERVICE_ENDPOINT_PREFIX + TRENDING_INFIX + CUSTOM_QUERY_ENDPOINT,
                RecoResult.class, RecoResult.EMPTY_RESULTS);
        } catch (RuntimeException e) {
            throw new MiruQueryServiceException("Failed score trending stream", e);
        } finally {
            processMetrics.stop();
        }
    }


    @Override
    public RecoResult collaborativeFilteringRecommendations(MiruPartitionId partitionId, RecoQuery query, Optional<RecoResult> lastResult) throws MiruQueryServiceException {
        MiruRecoQueryAndResultParams params = new MiruRecoQueryAndResultParams(query, lastResult.orNull());
        processMetrics.start();
        try {
            return requestHelper.executeRequest(params,
                QUERY_SERVICE_ENDPOINT_PREFIX + TRENDING_INFIX + CUSTOM_QUERY_ENDPOINT + "/" + partitionId.getId(),
                RecoResult.class, RecoResult.EMPTY_RESULTS);
        } catch (RuntimeException e) {
            throw new MiruQueryServiceException("Failed score trending stream for partition: " + partitionId.getId(), e);
        } finally {
            processMetrics.stop();
        }
    }
}
