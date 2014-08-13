package com.jivesoftware.os.miru.reader.naive;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.collect.Lists;
import com.jivesoftware.os.jive.utils.http.client.HttpClient;
import com.jivesoftware.os.jive.utils.http.client.HttpClientConfig;
import com.jivesoftware.os.jive.utils.http.client.HttpClientConfiguration;
import com.jivesoftware.os.jive.utils.http.client.HttpClientFactory;
import com.jivesoftware.os.jive.utils.http.client.HttpClientFactoryProvider;
import com.jivesoftware.os.jive.utils.http.client.rest.RequestHelper;
import com.jivesoftware.os.miru.api.MiruActorId;
import com.jivesoftware.os.miru.api.MiruAggregateCountsQueryCriteria;
import com.jivesoftware.os.miru.api.MiruDistinctCountQueryCriteria;
import com.jivesoftware.os.miru.api.MiruQueryServiceException;
import com.jivesoftware.os.miru.api.MiruReader;
import com.jivesoftware.os.miru.api.MiruRecoQueryCriteria;
import com.jivesoftware.os.miru.api.MiruTrendingQueryCriteria;
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
import com.jivesoftware.os.miru.reader.MiruHttpClientReader;
import com.jivesoftware.os.miru.reader.MiruHttpClientReaderConfig;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import javax.annotation.Nullable;
import javax.inject.Inject;
import javax.inject.Singleton;

@Singleton
public class MiruNaiveMultiClientRoundRobinReader implements MiruReader {

    private final List<MiruReader> readers;
    private final AtomicInteger roundRobinCounter = new AtomicInteger();

    @Inject
    public MiruNaiveMultiClientRoundRobinReader(final MiruHttpClientReaderConfig config, final ObjectMapper objectMapper) {
        Collection<HttpClientConfiguration> configurations = Lists.newArrayList();

        HttpClientConfig baseConfig = HttpClientConfig.newBuilder()
            .setSocketTimeoutInMillis(config.getSocketTimeoutInMillis())
            .setMaxConnections(config.getMaxConnections())
            .build();
        configurations.add(baseConfig);

        final HttpClientFactory createHttpClientFactory = new HttpClientFactoryProvider().createHttpClientFactory(configurations);

        readers = Lists.transform(Lists.newArrayList(config.getDefaultHostAddresses().split("\\s*,\\s*")), new Function<String, MiruReader>() {
            @Nullable
            @Override
            public MiruReader apply(@Nullable String input) {
                String[] hostPort = input.split("\\s*:\\s*");
                HttpClient httpClient = createHttpClientFactory.createClient(hostPort[0], Integer.parseInt(hostPort[1]));
                return new MiruHttpClientReader(new RequestHelper(httpClient, objectMapper));
            }
        });
    }

    @Override
    public AggregateCountsResult filterCustomStream(MiruTenantId tenantId, Optional<MiruActorId> userIdentity, Optional<MiruAuthzExpression> authzExpression,
        MiruAggregateCountsQueryCriteria queryCriteria) throws MiruQueryServiceException {

        return nextReader().filterCustomStream(tenantId, userIdentity, authzExpression, queryCriteria);
    }

    @Override
    public AggregateCountsResult filterInboxStreamAll(MiruTenantId tenantId, Optional<MiruActorId> userIdentity, Optional<MiruAuthzExpression> authzExpression,
        MiruAggregateCountsQueryCriteria queryCriteria) throws MiruQueryServiceException {

        return nextReader().filterInboxStreamAll(tenantId, userIdentity, authzExpression, queryCriteria);
    }

    @Override
    public AggregateCountsResult filterInboxStreamUnread(MiruTenantId tenantId, Optional<MiruActorId> userIdentity,
        Optional<MiruAuthzExpression> authzExpression, MiruAggregateCountsQueryCriteria queryCriteria) throws MiruQueryServiceException {

        return nextReader().filterInboxStreamUnread(tenantId, userIdentity, authzExpression, queryCriteria);
    }

    @Override
    public AggregateCountsResult filterCustomStream(MiruPartitionId partitionId, AggregateCountsQuery query, Optional<AggregateCountsResult> lastResult)
        throws MiruQueryServiceException {

        throw new UnsupportedOperationException("Direct host communication is not supported with this MiruReader.");
    }

    @Override
    public AggregateCountsResult filterInboxStreamAll(MiruPartitionId partitionId, AggregateCountsQuery query, Optional<AggregateCountsResult> lastResult)
        throws MiruQueryServiceException {

        throw new UnsupportedOperationException("Direct host communication is not supported with this MiruReader.");
    }

    @Override
    public AggregateCountsResult filterInboxStreamUnread(MiruPartitionId partitionId, AggregateCountsQuery query, Optional<AggregateCountsResult> lastResult)
        throws MiruQueryServiceException {

        throw new UnsupportedOperationException("Direct host communication is not supported with this MiruReader.");
    }

    @Override
    public DistinctCountResult countCustomStream(MiruTenantId tenantId, Optional<MiruActorId> userIdentity, Optional<MiruAuthzExpression> authzExpression,
        MiruDistinctCountQueryCriteria queryCriteria) throws MiruQueryServiceException {

        return nextReader().countCustomStream(tenantId, userIdentity, authzExpression, queryCriteria);
    }

    @Override
    public DistinctCountResult countInboxStreamAll(MiruTenantId tenantId, Optional<MiruActorId> userIdentity, Optional<MiruAuthzExpression> authzExpression,
        MiruDistinctCountQueryCriteria queryCriteria) throws MiruQueryServiceException {

        return nextReader().countInboxStreamAll(tenantId, userIdentity, authzExpression, queryCriteria);
    }

    @Override
    public DistinctCountResult countInboxStreamUnread(MiruTenantId tenantId, Optional<MiruActorId> userIdentity, Optional<MiruAuthzExpression> authzExpression,
        MiruDistinctCountQueryCriteria queryCriteria) throws MiruQueryServiceException {

        return nextReader().countInboxStreamUnread(tenantId, userIdentity, authzExpression, queryCriteria);
    }

    @Override
    public DistinctCountResult countCustomStream(MiruPartitionId partitionId, DistinctCountQuery query, Optional<DistinctCountResult> lastResult)
        throws MiruQueryServiceException {

        throw new UnsupportedOperationException("Direct host communication is not supported with this MiruReader.");
    }

    @Override
    public DistinctCountResult countInboxStreamAll(MiruPartitionId partitionId, DistinctCountQuery query, Optional<DistinctCountResult> lastResult)
        throws MiruQueryServiceException {

        throw new UnsupportedOperationException("Direct host communication is not supported with this MiruReader.");
    }

    @Override
    public DistinctCountResult countInboxStreamUnread(MiruPartitionId partitionId, DistinctCountQuery query, Optional<DistinctCountResult> lastResult)
        throws MiruQueryServiceException {

        throw new UnsupportedOperationException("Direct host communication is not supported with this MiruReader.");
    }

    @Override
    public TrendingResult scoreTrending(MiruTenantId tenantId, Optional<MiruActorId> userIdentity, Optional<MiruAuthzExpression> authzExpression,
        MiruTrendingQueryCriteria queryCriteria) throws MiruQueryServiceException {

        return nextReader().scoreTrending(tenantId, userIdentity, authzExpression, queryCriteria);
    }

    @Override
    public TrendingResult scoreTrending(MiruPartitionId partitionId, TrendingQuery query, Optional<TrendingResult> lastResult)
        throws MiruQueryServiceException {

        throw new UnsupportedOperationException("Direct host communication is not supported with this MiruReader.");
    }

    @Override
    public RecoResult collaborativeFilteringRecommendations(MiruTenantId tenantId,
            Optional<MiruActorId> userIdentity,
            Optional<MiruAuthzExpression> authzExpression, MiruRecoQueryCriteria queryCriteria) throws MiruQueryServiceException {
        return nextReader().collaborativeFilteringRecommendations(tenantId, userIdentity, authzExpression, queryCriteria);
    }


    @Override
    public RecoResult collaborativeFilteringRecommendations(MiruPartitionId partitionId, RecoQuery query, Optional<RecoResult> lastResult)
            throws MiruQueryServiceException {
        throw new UnsupportedOperationException("Direct host communication is not supported with this MiruReader.");
    }

    @Override
    public void warm(MiruTenantId tenantId) throws MiruQueryServiceException {
        nextReader().warm(tenantId);
    }

    private MiruReader nextReader() {
        return readers.get(roundRobinCounter.getAndIncrement() % readers.size());
    }
}