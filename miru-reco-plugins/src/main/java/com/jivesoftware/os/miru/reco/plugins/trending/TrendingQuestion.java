package com.jivesoftware.os.miru.reco.plugins.trending;

import com.google.common.base.Optional;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.jivesoftware.os.miru.analytics.plugins.analytics.Analytics;
import com.jivesoftware.os.miru.analytics.plugins.analytics.AnalyticsAnswer;
import com.jivesoftware.os.miru.analytics.plugins.analytics.AnalyticsQuery;
import com.jivesoftware.os.miru.analytics.plugins.analytics.AnalyticsQuestion;
import com.jivesoftware.os.miru.analytics.plugins.analytics.AnalyticsReport;
import com.jivesoftware.os.miru.api.MiruQueryServiceException;
import com.jivesoftware.os.miru.api.activity.MiruPartitionId;
import com.jivesoftware.os.miru.api.field.MiruFieldType;
import com.jivesoftware.os.miru.api.query.filter.MiruFieldFilter;
import com.jivesoftware.os.miru.api.query.filter.MiruFilter;
import com.jivesoftware.os.miru.api.query.filter.MiruFilterOperation;
import com.jivesoftware.os.miru.plugin.solution.MiruPartitionResponse;
import com.jivesoftware.os.miru.plugin.solution.MiruRemotePartition;
import com.jivesoftware.os.miru.plugin.solution.MiruRequest;
import com.jivesoftware.os.miru.plugin.solution.MiruRequestHandle;
import com.jivesoftware.os.miru.plugin.solution.MiruTimeRange;
import com.jivesoftware.os.miru.plugin.solution.Question;
import com.jivesoftware.os.miru.reco.plugins.distincts.Distincts;
import com.jivesoftware.os.miru.reco.plugins.distincts.DistinctsAnswer;
import com.jivesoftware.os.miru.reco.plugins.distincts.DistinctsQuery;
import com.jivesoftware.os.miru.reco.plugins.distincts.DistinctsQuestion;
import com.jivesoftware.os.miru.reco.plugins.distincts.DistinctsReport;
import com.jivesoftware.os.routing.bird.http.client.HttpClient;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Set;

/**
 *
 */
public class TrendingQuestion implements Question<TrendingQuery, AnalyticsAnswer, TrendingReport> {

    private final Distincts distincts;
    private final Analytics analytics;
    private final MiruRequest<TrendingQuery> request;
    private final MiruTimeRange combinedTimeRange;
    private final MiruRemotePartition<TrendingQuery, AnalyticsAnswer, TrendingReport> remotePartition;

    public TrendingQuestion(Distincts distincts,
        Analytics analytics,
        MiruTimeRange combinedTimeRange,
        MiruRequest<TrendingQuery> request,
        MiruRemotePartition<TrendingQuery, AnalyticsAnswer, TrendingReport> remotePartition) {
        this.distincts = distincts;
        this.analytics = analytics;
        this.combinedTimeRange = combinedTimeRange;
        this.request = request;
        this.remotePartition = remotePartition;
    }

    @Override
    public <BM> MiruPartitionResponse<AnalyticsAnswer> askLocal(MiruRequestHandle<BM, ?> handle, Optional<TrendingReport> report) throws Exception {
        Iterable<String> distinctTerms = null;
        if (request.query.distinctQueries.isEmpty()) {
            distinctTerms = Collections.emptyList();
        } else if (request.query.distinctQueries.size() == 1) {
            DistinctsQuestion distinctsQuestion = new DistinctsQuestion(distincts, new MiruRequest<>(
                request.tenantId,
                request.actorId,
                request.authzExpression,
                request.query.distinctQueries.get(0),
                request.logLevel),
                null); //TODO hacky
            MiruPartitionResponse<DistinctsAnswer> distinctsResponse = distinctsQuestion.askLocal(handle, Optional.<DistinctsReport>absent());
            distinctTerms = (distinctsResponse.answer != null && distinctsResponse.answer.results != null)
                ? Sets.newHashSet(distinctsResponse.answer.results)
                : Collections.<String>emptySet();
        } else {
            Set<String> joinTerms = null;
            for (DistinctsQuery distinctQuery : request.query.distinctQueries) {
                DistinctsQuestion distinctsQuestion = new DistinctsQuestion(distincts, new MiruRequest<>(
                    request.tenantId,
                    request.actorId,
                    request.authzExpression,
                    distinctQuery,
                    request.logLevel),
                    null); //TODO hacky
                MiruPartitionResponse<DistinctsAnswer> distinctsResponse = distinctsQuestion.askLocal(handle, Optional.<DistinctsReport>absent());
                Set<String> queryTerms = (distinctsResponse.answer != null && distinctsResponse.answer.results != null)
                    ? Sets.newHashSet(distinctsResponse.answer.results)
                    : Collections.<String>emptySet();
                if (joinTerms == null) {
                    joinTerms = queryTerms;
                } else {
                    joinTerms.retainAll(queryTerms);
                }
            }
            distinctTerms = joinTerms == null ? Collections.emptySet() : joinTerms;
        }

        Map<String, MiruFilter> constraintsFilters = Maps.newHashMap();
        for (String term : distinctTerms) {
            constraintsFilters.put(term,
                new MiruFilter(MiruFilterOperation.and,
                    false,
                    Collections.singletonList(new MiruFieldFilter(
                        MiruFieldType.primary, request.query.aggregateCountAroundField, Collections.singletonList(term))),
                    null));
        }

        AnalyticsQuestion analyticsQuestion = new AnalyticsQuestion(analytics, new MiruRequest<>(
            request.tenantId,
            request.actorId,
            request.authzExpression,
            new AnalyticsQuery(combinedTimeRange,
                request.query.divideTimeRangeIntoNSegments,
                request.query.constraintsFilter,
                constraintsFilters),
            request.logLevel),
            null); //TODO hacky
        return analyticsQuestion.askLocal(handle, Optional.<AnalyticsReport>absent());
    }

    @Override
    public MiruPartitionResponse<AnalyticsAnswer> askRemote(HttpClient httpClient,
        MiruPartitionId partitionId,
        Optional<TrendingReport> report) throws MiruQueryServiceException {
        return remotePartition.askRemote(httpClient, partitionId, request, report);
    }

    @Override
    public Optional<TrendingReport> createReport(Optional<AnalyticsAnswer> answer) {
        Optional<TrendingReport> report = Optional.absent();
        if (answer.isPresent()) {
            report = Optional.of(new TrendingReport());
        }
        return report;
    }
}
