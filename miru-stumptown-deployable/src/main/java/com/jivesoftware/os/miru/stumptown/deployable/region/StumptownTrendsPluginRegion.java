package com.jivesoftware.os.miru.stumptown.deployable.region;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.google.common.base.Joiner;
import com.google.common.base.Optional;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.jivesoftware.os.miru.api.MiruActorId;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.api.query.filter.MiruAuthzExpression;
import com.jivesoftware.os.miru.api.query.filter.MiruFieldFilter;
import com.jivesoftware.os.miru.api.query.filter.MiruFilter;
import com.jivesoftware.os.miru.api.query.filter.MiruFilterOperation;
import com.jivesoftware.os.miru.api.query.filter.MiruValue;
import com.jivesoftware.os.miru.plugin.solution.MiruRequest;
import com.jivesoftware.os.miru.plugin.solution.MiruResponse;
import com.jivesoftware.os.miru.plugin.solution.MiruSolutionLogLevel;
import com.jivesoftware.os.miru.plugin.solution.MiruTimeRange;
import com.jivesoftware.os.miru.plugin.solution.Waveform;
import com.jivesoftware.os.miru.reco.plugins.distincts.DistinctsQuery;
import com.jivesoftware.os.miru.reco.plugins.trending.TrendingAnswer;
import com.jivesoftware.os.miru.reco.plugins.trending.TrendingAnswerScoreSet;
import com.jivesoftware.os.miru.reco.plugins.trending.TrendingConstants;
import com.jivesoftware.os.miru.reco.plugins.trending.TrendingQuery;
import com.jivesoftware.os.miru.reco.plugins.trending.TrendingQuery.Strategy;
import com.jivesoftware.os.miru.reco.plugins.trending.TrendingQueryScoreSet;
import com.jivesoftware.os.miru.reco.plugins.trending.Trendy;
import com.jivesoftware.os.miru.stumptown.deployable.StumptownSchemaConstants;
import com.jivesoftware.os.miru.stumptown.deployable.endpoints.MinMaxDouble;
import com.jivesoftware.os.miru.ui.MiruPageRegion;
import com.jivesoftware.os.miru.ui.MiruSoyRenderer;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import com.jivesoftware.os.routing.bird.http.client.HttpResponse;
import com.jivesoftware.os.routing.bird.http.client.HttpResponseMapper;
import com.jivesoftware.os.routing.bird.http.client.RoundRobinStrategy;
import com.jivesoftware.os.routing.bird.http.client.TenantAwareHttpClient;
import com.jivesoftware.os.routing.bird.shared.ClientCall.ClientResponse;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 *
 */
// soy.stumptown.page.trends§PluginRegion
public class StumptownTrendsPluginRegion implements MiruPageRegion<Optional<StumptownTrendsPluginRegion.TrendingPluginRegionInput>> {

    private static final MetricLogger log = MetricLoggerFactory.getLogger();

    private final String template;
    private final MiruSoyRenderer renderer;
    private final TenantAwareHttpClient<String> readerClient;
    private final ObjectMapper requestMapper;
    private final HttpResponseMapper responseMapper;

    public StumptownTrendsPluginRegion(String template,
        MiruSoyRenderer renderer,
        TenantAwareHttpClient<String> readerClient,
        ObjectMapper requestMapper,
        HttpResponseMapper responseMapper) {
        this.template = template;
        this.renderer = renderer;
        this.readerClient = readerClient;
        this.requestMapper = requestMapper;
        this.responseMapper = responseMapper;
    }

    public static class TrendingPluginRegionInput {

        final String logLevels;

        final int fromAgo;
        final int toAgo;
        final String fromTimeUnit;
        final String toTimeUnit;
        final int buckets;

        final String service;
        final String aggregateAroundField;

        final String strategy;

        public TrendingPluginRegionInput(String logLevels, int fromAgo, int toAgo, String fromTimeUnit, String toTimeUnit, int buckets, String service,
            String aggregateAroundField,
            String strategy) {
            this.logLevels = logLevels;
            this.fromAgo = fromAgo;
            this.toAgo = toAgo;
            this.fromTimeUnit = fromTimeUnit;
            this.toTimeUnit = toTimeUnit;
            this.buckets = buckets;
            this.service = service;
            this.aggregateAroundField = aggregateAroundField;
            this.strategy = strategy;
        }

    }

    @Override
    public String render(Optional<TrendingPluginRegionInput> optionalInput) {
        Map<String, Object> data = Maps.newHashMap();
        try {
            if (optionalInput.isPresent()) {
                TrendingPluginRegionInput input = optionalInput.get();
                int fromAgo = input.fromAgo > input.toAgo ? input.fromAgo : input.toAgo;
                int toAgo = input.fromAgo > input.toAgo ? input.toAgo : input.fromAgo;

                Set<String> logLevelSet = Sets.newHashSet(Splitter.on(',').split(input.logLevels));
                data.put("logLevels", ImmutableMap.of(
                    "trace", logLevelSet.contains("TRACE"),
                    "debug", logLevelSet.contains("DEBUG"),
                    "info", logLevelSet.contains("INFO"),
                    "warn", logLevelSet.contains("WARN"),
                    "error", logLevelSet.contains("ERROR")));

                data.put("logLevelList", input.logLevels);

                data.put("fromTimeUnit", input.fromTimeUnit);
                data.put("fromAgo", String.valueOf(fromAgo));
                data.put("toAgo", String.valueOf(toAgo));
                data.put("toTimeUnit", input.toTimeUnit);
                data.put("buckets", String.valueOf(input.buckets));
                data.put("service", input.service);
                data.put("aggregateAroundField", input.aggregateAroundField);
                data.put("aggregatableFields", Arrays.asList("service", "instance", "level", "thread", "logger", "exceptionClass", "methodName", "lineNumber"));

                data.put("strategy", input.strategy);
                data.put("strategyFields", Arrays.asList(Strategy.LEADER.name(), Strategy.LINEAR_REGRESSION.name()));

                TimeUnit fromTimeUnit = TimeUnit.valueOf(input.fromTimeUnit);
                TimeUnit toTimeUnit = TimeUnit.valueOf(input.toTimeUnit);
                MiruTimeRange miruTimeRange = QueryUtils.toMiruTimeRange(fromAgo, fromTimeUnit, toAgo, toTimeUnit, input.buckets);

                List<MiruFieldFilter> fieldFilters = Lists.newArrayList();

                QueryUtils.addFieldFilter(fieldFilters, fieldFilters, "level", input.logLevels);
                if (input.service != null) {
                    QueryUtils.addFieldFilter(fieldFilters, fieldFilters, "service", input.service);
                }

                MiruFilter constraintsFilter = new MiruFilter(MiruFilterOperation.and, false, fieldFilters, null);

                Strategy strategy = Strategy.valueOf(input.strategy);
                MiruResponse<TrendingAnswer> response = null;
                MiruTenantId tenantId = StumptownSchemaConstants.TENANT_ID;
                String endpoint = TrendingConstants.TRENDING_PREFIX + TrendingConstants.CUSTOM_QUERY_ENDPOINT;
                String request = requestMapper.writeValueAsString(new MiruRequest<>("stumptownTrends",
                    tenantId,
                    MiruActorId.NOT_PROVIDED,
                    MiruAuthzExpression.NOT_PROVIDED,
                    new TrendingQuery(
                        Collections.singletonList(new TrendingQueryScoreSet(
                            "stumptown",
                            Collections.singleton(strategy),
                            miruTimeRange,
                            30,
                            100)),
                        constraintsFilter,
                        input.aggregateAroundField,
                        Collections.singletonList(Collections.singletonList(new DistinctsQuery(
                            miruTimeRange,
                            input.aggregateAroundField,
                            null,
                            constraintsFilter,
                            null)))),
                    MiruSolutionLogLevel.INFO));

                MiruResponse<TrendingAnswer> trendingResponse = readerClient.call("",
                    new RoundRobinStrategy(),
                    "stumptownTrends",
                    httpClient -> {
                        HttpResponse httpResponse = httpClient.postJson(endpoint, request, null);
                        @SuppressWarnings("unchecked")
                        MiruResponse<TrendingAnswer> extractResponse = responseMapper.extractResultFromResponse(httpResponse,
                            MiruResponse.class,
                            new Class[]{TrendingAnswer.class},
                            null);
                        return new ClientResponse<>(extractResponse, true);
                    });
                if (trendingResponse != null && trendingResponse.answer != null) {
                    response = trendingResponse;
                } else {
                    log.warn("Empty trending response from {}", tenantId);
                }

                if (response != null && response.answer != null) {
                    data.put("elapse", String.valueOf(response.totalElapsed));

                    List<Trendy> results = null;
                    TrendingAnswerScoreSet answerScoreSets = response.answer.scoreSets.get("stumptown");
                    if (answerScoreSets != null) {
                        results = answerScoreSets.results.get(strategy.name());
                    }
                    if (results == null) {
                        results = Collections.emptyList();
                        log.warn("Empty score sets result from {}", strategy.name());
                    }

                    data.put("elapse", String.valueOf(response.totalElapsed));

                    final MinMaxDouble mmd = new MinMaxDouble();
                    mmd.value(0);
                    Map<String, long[]> pngWaveforms = Maps.newHashMap();

                    List<Waveform> answerWaveforms = response.answer.waveforms.get("stumptown");
                    if (answerWaveforms == null) {
                        log.warn("Empty waveform answer from stumptown");
                    } else {
                        Map<MiruValue, Waveform> waveforms = Maps.uniqueIndex(answerWaveforms, Waveform::getId);
                        for (Trendy t : results) {
                            long[] waveform = new long[input.buckets];
                            waveforms.get(t.distinctValue).mergeWaveform(waveform);
                            for (long w : waveform) {
                                mmd.value(w);
                            }
                            pngWaveforms.put(t.distinctValue.last(), waveform);
                        }
                    }

                    data.put("results", Lists.transform(results, trendy -> ImmutableMap.of(
                        "name", trendy.distinctValue.last(),
                        "rank", String.valueOf(Math.round(trendy.rank * 100.0) / 100.0),
                        "waveform", "data:image/png;base64," + new PNGWaveforms()
                        .hitsToBase64PNGWaveform(600, 96, 10, 4,
                            ImmutableMap.of(trendy.distinctValue.last(), pngWaveforms.get(trendy.distinctValue.last())),
                            Optional.of(mmd)))));
                    ObjectMapper mapper = new ObjectMapper();
                    mapper.enable(SerializationFeature.INDENT_OUTPUT);
                    data.put("summary", Joiner.on("\n").join(response.log) + "\n\n" + mapper.writeValueAsString(response.solutions));
                }
            }
        } catch (Exception e) {
            log.error("Unable to retrieve data", e);
        }

        return renderer.render(template, data);
    }

    @Override
    public String getTitle() {
        return "Trending";
    }
}
