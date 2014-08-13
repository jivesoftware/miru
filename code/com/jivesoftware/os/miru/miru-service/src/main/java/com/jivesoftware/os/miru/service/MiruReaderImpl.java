package com.jivesoftware.os.miru.service;

import com.google.common.base.Optional;
import com.jivesoftware.os.miru.api.MiruActorId;
import com.jivesoftware.os.miru.api.MiruAggregateCountsQueryCriteria;
import com.jivesoftware.os.miru.api.MiruDistinctCountQueryCriteria;
import com.jivesoftware.os.miru.api.MiruQueryServiceException;
import com.jivesoftware.os.miru.api.MiruReader;
import com.jivesoftware.os.miru.api.MiruRecoQueryCriteria;
import com.jivesoftware.os.miru.api.MiruTrendingQueryCriteria;
import com.jivesoftware.os.miru.api.activity.MiruPartitionId;
import com.jivesoftware.os.miru.api.base.MiruStreamId;
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
import com.jivesoftware.os.miru.service.partition.MiruPartitionUnavailableException;
import javax.inject.Inject;
import javax.inject.Singleton;

/**
 *
 */
@Singleton
public class MiruReaderImpl implements MiruReader {

    private final MiruService miruService;

    @Inject
    public MiruReaderImpl(MiruService miruService) {
        this.miruService = miruService;
    }

    @Override
    public AggregateCountsResult filterCustomStream(MiruTenantId tenantId, Optional<MiruActorId> userIdentity,
            Optional<MiruAuthzExpression> authzExpression, MiruAggregateCountsQueryCriteria queryCriteria) throws MiruQueryServiceException {
        try {
            return miruService.filterCustomStream(buildAggregateCountsQuery(tenantId, userIdentity, authzExpression, queryCriteria));
        } catch (MiruPartitionUnavailableException e) {
            throw e;
        } catch (Exception e) {
            //TODO throw http error codes
            throw new MiruQueryServiceException("Failed to filter custom stream", e);
        }
    }

    @Override
    public AggregateCountsResult filterInboxStreamAll(MiruTenantId tenantId, Optional<MiruActorId> userIdentity,
            Optional<MiruAuthzExpression> authzExpression, MiruAggregateCountsQueryCriteria queryCriteria) throws MiruQueryServiceException {
        try {
            return miruService.filterInboxStreamAll(buildAggregateCountsQuery(tenantId, userIdentity, authzExpression, queryCriteria));
        } catch (MiruPartitionUnavailableException e) {
            throw e;
        } catch (Exception e) {
            //TODO throw http error codes
            throw new MiruQueryServiceException("Failed to filter inbox all stream", e);
        }
    }

    @Override
    public AggregateCountsResult filterInboxStreamUnread(MiruTenantId tenantId, Optional<MiruActorId> userIdentity,
            Optional<MiruAuthzExpression> authzExpression, MiruAggregateCountsQueryCriteria queryCriteria) throws MiruQueryServiceException {
        try {
            return miruService.filterInboxStreamUnread(buildAggregateCountsQuery(tenantId, userIdentity, authzExpression, queryCriteria));
        } catch (MiruPartitionUnavailableException e) {
            throw e;
        } catch (Exception e) {
            //TODO throw http error codes
            throw new MiruQueryServiceException("Failed to filter inbox unread stream", e);
        }
    }

    @Override
    public AggregateCountsResult filterCustomStream(MiruPartitionId partitionId, AggregateCountsQuery query, Optional<AggregateCountsResult> lastResult)
            throws MiruQueryServiceException {
        try {
            return miruService.filterCustomStream(partitionId, query, lastResult);
        } catch (MiruPartitionUnavailableException e) {
            throw e;
        } catch (Exception e) {
            //TODO throw http error codes
            throw new MiruQueryServiceException("Failed to filter custom stream for partition: " + partitionId.getId(), e);
        }
    }

    @Override
    public AggregateCountsResult filterInboxStreamAll(MiruPartitionId partitionId, AggregateCountsQuery query, Optional<AggregateCountsResult> lastResult)
            throws MiruQueryServiceException {
        try {
            return miruService.filterInboxStreamAll(partitionId, query, lastResult);
        } catch (MiruPartitionUnavailableException e) {
            throw e;
        } catch (Exception e) {
            //TODO throw http error codes
            throw new MiruQueryServiceException("Failed to filter inbox all stream for partition: " + partitionId.getId(), e);
        }
    }

    @Override
    public AggregateCountsResult filterInboxStreamUnread(MiruPartitionId partitionId, AggregateCountsQuery query, Optional<AggregateCountsResult> lastResult)
            throws MiruQueryServiceException {
        try {
            return miruService.filterInboxStreamUnread(partitionId, query, lastResult);
        } catch (MiruPartitionUnavailableException e) {
            throw e;
        } catch (Exception e) {
            //TODO throw http error codes
            throw new MiruQueryServiceException("Failed to filter inbox unread stream for partition: " + partitionId.getId(), e);
        }
    }

    @Override
    public DistinctCountResult countCustomStream(MiruTenantId tenantId, Optional<MiruActorId> userIdentity,
            Optional<MiruAuthzExpression> authzExpression, MiruDistinctCountQueryCriteria queryCriteria) throws MiruQueryServiceException {
        try {
            return miruService.countCustomStream(buildDistinctCountQuery(tenantId, authzExpression, queryCriteria));
        } catch (MiruPartitionUnavailableException e) {
            throw e;
        } catch (Exception e) {
            //TODO throw http error codes
            throw new MiruQueryServiceException("Failed to count custom stream", e);
        }
    }

    @Override
    public DistinctCountResult countInboxStreamAll(MiruTenantId tenantId, Optional<MiruActorId> userIdentity,
            Optional<MiruAuthzExpression> authzExpression, MiruDistinctCountQueryCriteria queryCriteria) throws MiruQueryServiceException {
        try {
            return miruService.countInboxStreamAll(buildDistinctCountQuery(tenantId, authzExpression, queryCriteria));
        } catch (MiruPartitionUnavailableException e) {
            throw e;
        } catch (Exception e) {
            //TODO throw http error codes
            throw new MiruQueryServiceException("Failed to count inbox all stream", e);
        }
    }

    @Override
    public DistinctCountResult countInboxStreamUnread(MiruTenantId tenantId, Optional<MiruActorId> userIdentity,
            Optional<MiruAuthzExpression> authzExpression, MiruDistinctCountQueryCriteria queryCriteria) throws MiruQueryServiceException {
        try {
            return miruService.countInboxStreamUnread(buildDistinctCountQuery(tenantId, authzExpression, queryCriteria));
        } catch (MiruPartitionUnavailableException e) {
            throw e;
        } catch (Exception e) {
            //TODO throw http error codes
            throw new MiruQueryServiceException("Failed to count inbox unread stream", e);
        }
    }

    @Override
    public DistinctCountResult countCustomStream(MiruPartitionId partitionId, DistinctCountQuery query, Optional<DistinctCountResult> lastResult)
            throws MiruQueryServiceException {
        try {
            return miruService.countCustomStream(partitionId, query, lastResult);
        } catch (MiruPartitionUnavailableException e) {
            throw e;
        } catch (Exception e) {
            //TODO throw http error codes
            throw new MiruQueryServiceException("Failed to count custom stream for partition: " + partitionId.getId(), e);
        }
    }

    @Override
    public DistinctCountResult countInboxStreamAll(MiruPartitionId partitionId, DistinctCountQuery query, Optional<DistinctCountResult> lastResult)
            throws MiruQueryServiceException {
        try {
            return miruService.countInboxStreamAll(partitionId, query, lastResult);
        } catch (MiruPartitionUnavailableException e) {
            throw e;
        } catch (Exception e) {
            //TODO throw http error codes
            throw new MiruQueryServiceException("Failed to count inbox all stream for partition: " + partitionId.getId(), e);
        }
    }

    @Override
    public DistinctCountResult countInboxStreamUnread(MiruPartitionId partitionId, DistinctCountQuery query, Optional<DistinctCountResult> lastResult)
            throws MiruQueryServiceException {
        try {
            return miruService.countInboxStreamUnread(partitionId, query, lastResult);
        } catch (MiruPartitionUnavailableException e) {
            throw e;
        } catch (Exception e) {
            //TODO throw http error codes
            throw new MiruQueryServiceException("Failed to count inbox unread stream for partition: " + partitionId.getId(), e);
        }
    }

    @Override
    public TrendingResult scoreTrending(MiruTenantId tenantId, Optional<MiruActorId> userIdentity, Optional<MiruAuthzExpression> authzExpression,
            MiruTrendingQueryCriteria queryCriteria) throws MiruQueryServiceException {
        try {
            return miruService.scoreTrendingStream(buildTrendingQuery(tenantId, authzExpression, queryCriteria));
        } catch (MiruPartitionUnavailableException e) {
            throw e;
        } catch (Exception e) {
            //TODO throw http error codes
            throw new MiruQueryServiceException("Failed to score trending stream", e);
        }
    }

    @Override
    public TrendingResult scoreTrending(MiruPartitionId partitionId, TrendingQuery query, Optional<TrendingResult> lastResult)
            throws MiruQueryServiceException {
        try {
            return miruService.scoreTrendingStream(partitionId, query, lastResult);
        } catch (MiruPartitionUnavailableException e) {
            throw e;
        } catch (Exception e) {
            //TODO throw http error codes
            throw new MiruQueryServiceException("Failed to score trending stream for partition: " + partitionId.getId(), e);
        }
    }

    @Override
    public RecoResult collaborativeFilteringRecommendations(MiruTenantId tenantId,
            Optional<MiruActorId> userIdentity,
            Optional<MiruAuthzExpression> authzExpression, MiruRecoQueryCriteria queryCriteria) throws MiruQueryServiceException {
        try {
            return miruService.collaborativeFilteringRecommendations(buildRecoQuery(tenantId, authzExpression, queryCriteria));
        } catch (MiruPartitionUnavailableException e) {
            throw e;
        } catch (Exception e) {
            //TODO throw http error codes
            throw new MiruQueryServiceException("Failed to score trending stream", e);
        }
    }


    @Override
    public RecoResult collaborativeFilteringRecommendations(MiruPartitionId partitionId, RecoQuery query, Optional<RecoResult> lastResult)
            throws MiruQueryServiceException {
        try {
            return miruService.collaborativeFilteringRecommendations(partitionId, query, lastResult);
        } catch (MiruPartitionUnavailableException e) {
            throw e;
        } catch (Exception e) {
            //TODO throw http error codes
            throw new MiruQueryServiceException("Failed to score trending stream for partition: " + partitionId.getId(), e);
        }
    }

    @Override
    public void warm(MiruTenantId tenantId) throws MiruQueryServiceException {
        try {
            miruService.warm(tenantId);
        } catch (Exception e) {
            //TODO throw http error codes
            throw new MiruQueryServiceException("Failed to warm", e);
        }
    }

    private AggregateCountsQuery buildAggregateCountsQuery(MiruTenantId tenantId, Optional<MiruActorId> userIdentity,
            Optional<MiruAuthzExpression> authzExpression, MiruAggregateCountsQueryCriteria queryCriteria) {
        return new AggregateCountsQuery(
                tenantId,
                Optional.fromNullable(queryCriteria.getStreamId() != null ? new MiruStreamId(queryCriteria.getStreamId().toBytes()) : null),
                Optional.fromNullable(queryCriteria.getAnswerTimeRange()),
                Optional.fromNullable(queryCriteria.getCountTimeRange()),
                authzExpression,
                queryCriteria.getStreamFilter(),
                Optional.fromNullable(queryCriteria.getConstraintsFilter()),
                queryCriteria.getQuery(),
                queryCriteria.getAggregateCountAroundField(),
                queryCriteria.getStartFromDistinctN(),
                queryCriteria.getDesiredNumberOfDistincts());
    }

    private DistinctCountQuery buildDistinctCountQuery(MiruTenantId tenantId,
            Optional<MiruAuthzExpression> authzExpression,
            MiruDistinctCountQueryCriteria queryCriteria) {
        return new DistinctCountQuery(
                tenantId,
                Optional.fromNullable(queryCriteria.getStreamId() != null ? new MiruStreamId(queryCriteria.getStreamId().toBytes()) : null),
                Optional.fromNullable(queryCriteria.getTimeRange()),
                authzExpression,
                queryCriteria.getStreamFilter(),
                Optional.fromNullable(queryCriteria.getConstraintsFilter()),
                queryCriteria.getAggregateCountAroundField(),
                queryCriteria.getDesiredNumberOfDistincts());
    }

    private TrendingQuery buildTrendingQuery(MiruTenantId tenantId,
            Optional<MiruAuthzExpression> authzExpression,
            MiruTrendingQueryCriteria queryCriteria) {
        return new TrendingQuery(
                tenantId,
                authzExpression,
                queryCriteria.getConstraintsFilter(),
                queryCriteria.getAggregateCountAroundField(),
                queryCriteria.getDesiredNumberOfDistincts());
    }

    private RecoQuery buildRecoQuery(MiruTenantId tenantId,
            Optional<MiruAuthzExpression> authzExpression,
            MiruRecoQueryCriteria queryCriteria) {
        return new RecoQuery(
                tenantId,
                authzExpression,
                queryCriteria.getConstraintsFilter(),
                queryCriteria.getAggregateFieldName1(),
                queryCriteria.getRetrieveFieldName1(),
                queryCriteria.getLookupFieldNamed1(),
                queryCriteria.getAggregateFieldName2(),
                queryCriteria.getRetrieveFieldName2(),
                queryCriteria.getLookupFieldNamed2(),
                queryCriteria.getAggregateFieldName3(),
                queryCriteria.getRetrieveFieldName3(),
                queryCriteria.getDesiredNumberOfDistincts());
    }

}
