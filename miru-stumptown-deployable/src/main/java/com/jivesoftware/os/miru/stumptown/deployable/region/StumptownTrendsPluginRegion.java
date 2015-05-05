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
import com.jivesoftware.os.jive.utils.http.client.rest.RequestHelper;
import com.jivesoftware.os.miru.api.MiruActorId;
import com.jivesoftware.os.miru.api.MiruHost;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.api.query.filter.MiruAuthzExpression;
import com.jivesoftware.os.miru.api.query.filter.MiruFieldFilter;
import com.jivesoftware.os.miru.api.query.filter.MiruFilter;
import com.jivesoftware.os.miru.api.query.filter.MiruFilterOperation;
import com.jivesoftware.os.miru.api.topology.ReaderRequestHelpers;
import com.jivesoftware.os.miru.plugin.solution.MiruRequest;
import com.jivesoftware.os.miru.plugin.solution.MiruResponse;
import com.jivesoftware.os.miru.plugin.solution.MiruSolutionLogLevel;
import com.jivesoftware.os.miru.plugin.solution.MiruTimeRange;
import com.jivesoftware.os.miru.reco.plugins.trending.TrendingAnswer;
import com.jivesoftware.os.miru.reco.plugins.trending.TrendingConstants;
import com.jivesoftware.os.miru.reco.plugins.trending.TrendingQuery;
import com.jivesoftware.os.miru.reco.plugins.trending.Trendy;
import com.jivesoftware.os.miru.stumptown.deployable.MiruSoyRenderer;
import com.jivesoftware.os.miru.stumptown.deployable.StumptownSchemaConstants;
import com.jivesoftware.os.miru.stumptown.deployable.endpoints.MinMaxDouble;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
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
public class StumptownTrendsPluginRegion implements PageRegion<Optional<StumptownTrendsPluginRegion.TrendingPluginRegionInput>> {

    private static final MetricLogger log = MetricLoggerFactory.getLogger();

    private final String template;
    private final MiruSoyRenderer renderer;
    private final ReaderRequestHelpers miruReaders;

    public StumptownTrendsPluginRegion(String template,
        MiruSoyRenderer renderer,
        ReaderRequestHelpers miruReaders) {
        this.template = template;
        this.renderer = renderer;
        this.miruReaders = miruReaders;
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
                data.put("strategyFields", Arrays.asList(TrendingQuery.Strategy.LEADER.name(), TrendingQuery.Strategy.LINEAR_REGRESSION.name()));

                TimeUnit fromTimeUnit = TimeUnit.valueOf(input.fromTimeUnit);
                TimeUnit toTimeUnit = TimeUnit.valueOf(input.toTimeUnit);
                MiruTimeRange miruTimeRange = QueryUtils.toMiruTimeRange(fromAgo, fromTimeUnit, toAgo, toTimeUnit, input.buckets);

                List<MiruFieldFilter> fieldFilters = Lists.newArrayList();

                QueryUtils.addFieldFilter(fieldFilters, fieldFilters, "level", input.logLevels);
                if (input.service != null) {
                    QueryUtils.addFieldFilter(fieldFilters, fieldFilters, "service", input.service);
                }

                MiruFilter constraintsFilter = new MiruFilter(MiruFilterOperation.and, false, fieldFilters, null);

                MiruResponse<TrendingAnswer> response = null;
                MiruTenantId tenantId = StumptownSchemaConstants.TENANT_ID;
                for (RequestHelper requestHelper : miruReaders.get(Optional.<MiruHost>absent())) {
                    try {
                        @SuppressWarnings("unchecked")
                        MiruResponse<TrendingAnswer> trendingResponse = requestHelper.executeRequest(
                            new MiruRequest<>(tenantId, MiruActorId.NOT_PROVIDED, MiruAuthzExpression.NOT_PROVIDED,
                                new TrendingQuery(TrendingQuery.Strategy.valueOf(input.strategy),
                                    miruTimeRange,
                                    null,
                                    30,
                                    constraintsFilter,
                                    input.aggregateAroundField,
                                    MiruFilter.NO_FILTER,
                                    null,
                                    100),
                                MiruSolutionLogLevel.INFO),
                            TrendingConstants.TRENDING_PREFIX + TrendingConstants.CUSTOM_QUERY_ENDPOINT, MiruResponse.class,
                            new Class[] { TrendingAnswer.class },
                            null);
                        response = trendingResponse;
                        if (response != null && response.answer != null) {
                            break;
                        } else {
                            log.warn("Empty trending response from {}, trying another", requestHelper);
                        }
                    } catch (Exception e) {
                        log.warn("Failed trending request to {}, trying another", new Object[] { requestHelper }, e);
                    }
                }

                if (response != null && response.answer != null) {
                    data.put("elapse", String.valueOf(response.totalElapsed));

                    List<Trendy> results = response.answer.results;
                    if (results == null) {
                        results = Collections.emptyList();
                    }
                    data.put("elapse", String.valueOf(response.totalElapsed));
                    //data.put("waveform", waveform == null ? "" : waveform.toString());

                    final MinMaxDouble mmd = new MinMaxDouble();
                    mmd.value(0);
                    for (Trendy t : results) {
                        for (long w : t.waveform) {
                            mmd.value(w);
                        }
                    }

                    data.put("results", Lists.transform(results, trendy -> ImmutableMap.of(
                        "name", trendy.distinctValue,
                        "rank", String.valueOf(Math.round(trendy.rank * 100.0) / 100.0),
                        "waveform", "data:image/png;base64," + new PNGWaveforms()
                            .hitsToBase64PNGWaveform(600, 96, 10, 4,
                                ImmutableMap.of(trendy.distinctValue, trendy.waveform),
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
