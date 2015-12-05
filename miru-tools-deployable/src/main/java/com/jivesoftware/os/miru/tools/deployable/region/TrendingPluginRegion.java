package com.jivesoftware.os.miru.tools.deployable.region;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.google.common.base.Charsets;
import com.google.common.base.Functions;
import com.google.common.base.Joiner;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.jivesoftware.os.jive.utils.ordered.id.JiveEpochTimestampProvider;
import com.jivesoftware.os.jive.utils.ordered.id.SnowflakeIdPacker;
import com.jivesoftware.os.miru.api.MiruActorId;
import com.jivesoftware.os.miru.api.MiruHost;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.api.field.MiruFieldType;
import com.jivesoftware.os.miru.api.query.filter.FilterStringUtil;
import com.jivesoftware.os.miru.api.query.filter.MiruAuthzExpression;
import com.jivesoftware.os.miru.api.query.filter.MiruFieldFilter;
import com.jivesoftware.os.miru.api.query.filter.MiruFilter;
import com.jivesoftware.os.miru.api.query.filter.MiruFilterOperation;
import com.jivesoftware.os.miru.api.topology.ReaderRequestHelpers;
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
import com.jivesoftware.os.miru.reco.plugins.trending.Trendy;
import com.jivesoftware.os.miru.tools.deployable.analytics.MinMaxDouble;
import com.jivesoftware.os.miru.tools.deployable.analytics.PNGWaveforms;
import com.jivesoftware.os.miru.ui.MiruPageRegion;
import com.jivesoftware.os.miru.ui.MiruSoyRenderer;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import com.jivesoftware.os.routing.bird.http.client.HttpRequestHelper;
import java.util.Arrays;
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
    private final ReaderRequestHelpers readerRequestHelpers;
    private final FilterStringUtil filterStringUtil = new FilterStringUtil();

    public TrendingPluginRegion(String template,
        MiruSoyRenderer renderer,
        ReaderRequestHelpers readerRequestHelpers) {
        this.template = template;
        this.renderer = renderer;
        this.readerRequestHelpers = readerRequestHelpers;
    }

    public static class TrendingPluginRegionInput {

        final String tenant;
        final int fromHoursAgo;
        final int toHoursAgo;
        final int buckets;
        final String field;
        final String filter;
        final List<String> fieldPrefixes;
        final String distinctsFilter;
        final String logLevel;

        public TrendingPluginRegionInput(String tenant,
            int fromHoursAgo,
            int toHoursAgo,
            int buckets,
            String field,
            String filter,
            List<String> fieldPrefixes,
            String distinctsFilter,
            String logLevel) {
            this.tenant = tenant;
            this.fromHoursAgo = fromHoursAgo;
            this.toHoursAgo = toHoursAgo;
            this.buckets = buckets;
            this.field = field;
            this.filter = filter;
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
                data.put("filter", input.filter);
                data.put("fieldPrefixes", input.fieldPrefixes != null ? Joiner.on(", ").join(input.fieldPrefixes) : "");
                data.put("distinctsFilter", input.distinctsFilter);

                SnowflakeIdPacker snowflakeIdPacker = new SnowflakeIdPacker();
                long jiveCurrentTime = new JiveEpochTimestampProvider().getTimestamp();
                final long packCurrentTime = snowflakeIdPacker.pack(jiveCurrentTime, 0, 0);
                final long fromTime = packCurrentTime - snowflakeIdPacker.pack(TimeUnit.HOURS.toMillis(fromHoursAgo), 0, 0);
                final long toTime = packCurrentTime - snowflakeIdPacker.pack(TimeUnit.HOURS.toMillis(toHoursAgo), 0, 0);
                List<MiruFieldFilter> fieldFilters = Lists.newArrayList();
                fieldFilters.add(new MiruFieldFilter(MiruFieldType.primary, "activityType",
                    Lists.transform(
                        Arrays.asList(0, 1, 11, 65),
                        Functions.toStringFunction())));

                List<MiruFilter> constraintsSubFilters = null;
                if (!input.filter.isEmpty()) {
                    constraintsSubFilters = Collections.singletonList(filterStringUtil.parse(input.filter));
                }
                MiruFilter constraintsFilter = new MiruFilter(MiruFilterOperation.and, false, fieldFilters, constraintsSubFilters);

                MiruFilter distinctsFilter = input.distinctsFilter.isEmpty() ? MiruFilter.NO_FILTER : filterStringUtil.parse(input.distinctsFilter);

                List<HttpRequestHelper> requestHelpers = readerRequestHelpers.get(Optional.<MiruHost>absent());
                MiruResponse<TrendingAnswer> response = null;
                if (!input.tenant.trim().isEmpty()) {
                    MiruTenantId tenantId = new MiruTenantId(input.tenant.trim().getBytes(Charsets.UTF_8));
                    for (HttpRequestHelper requestHelper : requestHelpers) {
                        try {
                            MiruTimeRange timeRange = new MiruTimeRange(fromTime, toTime);
                            @SuppressWarnings("unchecked")
                            MiruResponse<TrendingAnswer> trendingResponse = requestHelper.executeRequest(
                                new MiruRequest<>("toolsTrending",
                                    tenantId,
                                    MiruActorId.NOT_PROVIDED,
                                    MiruAuthzExpression.NOT_PROVIDED,
                                    new TrendingQuery(
                                        Collections.singleton(Strategy.LINEAR_REGRESSION),
                                        timeRange,
                                        null,
                                        input.buckets,
                                        constraintsFilter,
                                        input.field,
                                        Collections.singletonList(new DistinctsQuery(
                                            timeRange,
                                            input.field,
                                            distinctsFilter,
                                            input.fieldPrefixes)),
                                        100),
                                    MiruSolutionLogLevel.valueOf(input.logLevel)),
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
                }

                if (response != null && response.answer != null) {
                    data.put("elapse", String.valueOf(response.totalElapsed));

                    Map<String, Waveform> waveforms = response.answer.waveforms;
                    List<Trendy> results = response.answer.results.get(Strategy.LINEAR_REGRESSION.name());
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
                        pngWaveforms.put(t.distinctValue, waveform);
                    }

                    data.put("results", Lists.transform(results, trendy -> ImmutableMap.of(
                        "name", trendy.distinctValue,
                        "rank", String.valueOf(trendy.rank),
                        "waveform", "data:image/png;base64," + new PNGWaveforms()
                            .hitsToBase64PNGWaveform(600, 128, 10,
                                ImmutableMap.of(trendy.distinctValue, pngWaveforms.get(trendy.distinctValue)),
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
