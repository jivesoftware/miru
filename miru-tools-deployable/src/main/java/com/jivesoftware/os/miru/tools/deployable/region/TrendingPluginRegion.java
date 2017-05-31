package com.jivesoftware.os.miru.tools.deployable.region;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.google.common.base.Charsets;
import com.google.common.base.Joiner;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.jivesoftware.os.jive.utils.ordered.id.JiveEpochTimestampProvider;
import com.jivesoftware.os.jive.utils.ordered.id.SnowflakeIdPacker;
import com.jivesoftware.os.miru.api.MiruActorId;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.api.query.filter.FilterStringUtil;
import com.jivesoftware.os.miru.api.query.filter.MiruAuthzExpression;
import com.jivesoftware.os.miru.api.query.filter.MiruFieldFilter;
import com.jivesoftware.os.miru.api.query.filter.MiruFilter;
import com.jivesoftware.os.miru.api.query.filter.MiruFilterOperation;
import com.jivesoftware.os.miru.api.query.filter.MiruValue;
import com.jivesoftware.os.miru.plugin.query.MiruRouting;
import com.jivesoftware.os.miru.plugin.solution.MiruRequest;
import com.jivesoftware.os.miru.plugin.solution.MiruResponse;
import com.jivesoftware.os.miru.plugin.solution.MiruSolutionLogLevel;
import com.jivesoftware.os.miru.plugin.solution.MiruTimeRange;
import com.jivesoftware.os.miru.plugin.solution.Waveform;
import com.jivesoftware.os.miru.reco.plugins.distincts.DistinctsQuery;
import com.jivesoftware.os.miru.reco.plugins.trending.TrendingAnswer;
import com.jivesoftware.os.miru.reco.plugins.trending.TrendingConstants;
import com.jivesoftware.os.miru.reco.plugins.trending.TrendingQuery;
import com.jivesoftware.os.miru.reco.plugins.trending.TrendingQuery.Strategy;
import com.jivesoftware.os.miru.reco.plugins.trending.TrendingQueryScoreSet;
import com.jivesoftware.os.miru.reco.plugins.trending.Trendy;
import com.jivesoftware.os.miru.tools.deployable.analytics.MinMaxDouble;
import com.jivesoftware.os.miru.tools.deployable.analytics.PNGWaveforms;
import com.jivesoftware.os.miru.ui.MiruPageRegion;
import com.jivesoftware.os.miru.ui.MiruSoyRenderer;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 *
 */
// soy.miru.page.trendingPluginRegion
public class TrendingPluginRegion implements MiruPageRegion<Optional<TrendingPluginRegion.TrendingPluginRegionInput>> {

    private static final MetricLogger log = MetricLoggerFactory.getLogger();

    private final String template;
    private final MiruSoyRenderer renderer;
    private final MiruRouting routing;
    private final FilterStringUtil filterStringUtil;

    public TrendingPluginRegion(String template,
        MiruSoyRenderer renderer,
        MiruRouting routing,
        FilterStringUtil filterStringUtil) {
        this.template = template;
        this.renderer = renderer;
        this.routing = routing;
        this.filterStringUtil = filterStringUtil;
    }

    public static class TrendingPluginRegionInput {

        final String tenant;
        final int fromHoursAgo;
        final int toHoursAgo;
        final int buckets;
        final String field;
        final String strategy;
        final String filter;
        final String subFilters;
        final List<String> fieldPrefixes;
        final String distinctsFilter;
        final String logLevel;

        public TrendingPluginRegionInput(String tenant,
            int fromHoursAgo,
            int toHoursAgo,
            int buckets,
            String field,
            String strategy,
            String filter,
            String subFilters,
            List<String> fieldPrefixes,
            String distinctsFilter,
            String logLevel) {
            this.tenant = tenant;
            this.fromHoursAgo = fromHoursAgo;
            this.toHoursAgo = toHoursAgo;
            this.buckets = buckets;
            this.field = field;
            this.strategy = strategy;
            this.filter = filter;
            this.subFilters = subFilters;
            this.fieldPrefixes = fieldPrefixes;
            this.distinctsFilter = distinctsFilter;
            this.logLevel = logLevel;
        }
    }

    @Override
    public String render(Optional<TrendingPluginRegionInput> optionalInput) {
        Map<String, Object> data = Maps.newHashMap();
        try {
            if (optionalInput.isPresent()) {
                TrendingPluginRegionInput input = optionalInput.get();
                int fromHoursAgo = input.fromHoursAgo > input.toHoursAgo ? input.fromHoursAgo : input.toHoursAgo;
                int toHoursAgo = input.fromHoursAgo > input.toHoursAgo ? input.toHoursAgo : input.fromHoursAgo;

                data.put("logLevel", input.logLevel);
                data.put("tenant", input.tenant);
                data.put("fromHoursAgo", String.valueOf(fromHoursAgo));
                data.put("toHoursAgo", String.valueOf(toHoursAgo));
                data.put("buckets", String.valueOf(input.buckets));
                data.put("field", input.field);
                data.put("strategy", input.strategy);
                data.put("filter", input.filter);
                data.put("subFilters", input.subFilters);
                data.put("fieldPrefixes", input.fieldPrefixes != null ? Joiner.on(", ").join(input.fieldPrefixes) : "");
                data.put("distinctsFilter", input.distinctsFilter);

                SnowflakeIdPacker snowflakeIdPacker = new SnowflakeIdPacker();
                long jiveCurrentTime = new JiveEpochTimestampProvider().getTimestamp();
                final long packCurrentTime = snowflakeIdPacker.pack(jiveCurrentTime, 0, 0);
                final long fromTime = packCurrentTime - snowflakeIdPacker.pack(TimeUnit.HOURS.toMillis(fromHoursAgo), 0, 0);
                final long toTime = packCurrentTime - snowflakeIdPacker.pack(TimeUnit.HOURS.toMillis(toHoursAgo), 0, 0);

                List<MiruFieldFilter> fieldFilters = null;
                if (!input.subFilters.isEmpty()) {
                    fieldFilters = Collections.singletonList(filterStringUtil.parseFilter(input.subFilters));
                }

                List<MiruFilter> constraintsSubFilters = null;
                if (!input.filter.isEmpty()) {
                    constraintsSubFilters = Collections.singletonList(filterStringUtil.parseFilters(input.filter));
                }
                MiruFilter constraintsFilter = MiruFilter.NO_FILTER;
                if (fieldFilters != null || constraintsSubFilters != null) {
                    constraintsFilter = new MiruFilter(MiruFilterOperation.and, false, fieldFilters, constraintsSubFilters);
                }

                MiruFilter distinctsFilter = input.distinctsFilter.isEmpty() ? MiruFilter.NO_FILTER : filterStringUtil.parseFilters(input.distinctsFilter);

                MiruResponse<TrendingAnswer> response = null;
                if (!input.tenant.trim().isEmpty()) {
                    MiruTenantId tenantId = new MiruTenantId(input.tenant.trim().getBytes(Charsets.UTF_8));
                    MiruTimeRange timeRange = new MiruTimeRange(fromTime, toTime);
                    String endpoint = TrendingConstants.TRENDING_PREFIX + TrendingConstants.CUSTOM_QUERY_ENDPOINT;
                    MiruRequest<TrendingQuery> miruRequest = new MiruRequest<>("toolsTrending",
                        tenantId,
                        MiruActorId.NOT_PROVIDED,
                        MiruAuthzExpression.NOT_PROVIDED,
                        new TrendingQuery(
                            Collections.singletonList(new TrendingQueryScoreSet("tools",
                                Collections.singleton(Strategy.valueOf(input.strategy)),
                                timeRange,
                                input.buckets,
                                100)),
                            constraintsFilter,
                            input.field,
                            Collections.singletonList(Collections.singletonList(new DistinctsQuery(
                                timeRange,
                                input.field,
                                null,
                                distinctsFilter,
                                filterStringUtil.buildFieldPrefixes(input.fieldPrefixes))))),
                        MiruSolutionLogLevel.valueOf(input.logLevel));

                    MiruResponse<TrendingAnswer> trendingResponse = routing.query("", "trendingPluginRegion",
                        miruRequest, endpoint, TrendingAnswer.class);

                    if (trendingResponse != null && trendingResponse.answer != null) {
                        response = trendingResponse;
                    } else {
                        log.warn("Empty trending response from {}", tenantId);
                    }
                }

                if (response != null && response.answer != null && response.answer.waveforms != null) {
                    data.put("elapse", String.valueOf(response.totalElapsed));

                    List<Waveform> answerWaveforms = response.answer.waveforms.get("tools");
                    Map<MiruValue, Waveform> waveforms = Maps.uniqueIndex(answerWaveforms, Waveform::getId);
                    List<Trendy> results = response.answer.scoreSets.get("tools").results.get(input.strategy);
                    if (results == null) {
                        results = Collections.emptyList();
                    }
                    data.put("elapse", String.valueOf(response.totalElapsed));
                    //data.put("waveform", waveform == null ? "" : waveform.toString());

                    final MinMaxDouble mmd = new MinMaxDouble();
                    mmd.value(0);
                    Map<String, long[]> pngWaveforms = Maps.newHashMap();
                    for (Trendy t : results) {
                        long[] waveform = new long[input.buckets];
                        waveforms.get(t.distinctValue).mergeWaveform(waveform);
                        for (long w : waveform) {
                            mmd.value(w);
                        }
                        pngWaveforms.put(t.distinctValue.last(), waveform);
                    }

                    data.put("results", Lists.transform(results, trendy -> ImmutableMap.of(
                        "name", trendy.distinctValue.last(),
                        "rank", String.valueOf(trendy.rank),
                        "waveform", "data:image/png;base64," + new PNGWaveforms()
                        .hitsToBase64PNGWaveform(600, 128, 10,
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
