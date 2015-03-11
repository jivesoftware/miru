package com.jivesoftware.os.miru.manage.deployable.region;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.google.common.base.Charsets;
import com.google.common.base.Function;
import com.google.common.base.Functions;
import com.google.common.base.Joiner;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.jivesoftware.os.jive.utils.http.client.rest.RequestHelper;
import com.jivesoftware.os.jive.utils.ordered.id.JiveEpochTimestampProvider;
import com.jivesoftware.os.jive.utils.ordered.id.SnowflakeIdPacker;
import com.jivesoftware.os.miru.analytics.plugins.analytics.AnalyticsAnswer;
import com.jivesoftware.os.miru.api.MiruActorId;
import com.jivesoftware.os.miru.api.MiruHost;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.api.field.MiruFieldType;
import com.jivesoftware.os.miru.api.query.filter.MiruAuthzExpression;
import com.jivesoftware.os.miru.api.query.filter.MiruFieldFilter;
import com.jivesoftware.os.miru.api.query.filter.MiruFilter;
import com.jivesoftware.os.miru.api.query.filter.MiruFilterOperation;
import com.jivesoftware.os.miru.cluster.client.ReaderRequestHelpers;
import com.jivesoftware.os.miru.manage.deployable.MiruSoyRenderer;
import com.jivesoftware.os.miru.manage.deployable.analytics.MinMaxDouble;
import com.jivesoftware.os.miru.plugin.solution.MiruRequest;
import com.jivesoftware.os.miru.plugin.solution.MiruResponse;
import com.jivesoftware.os.miru.plugin.solution.MiruSolutionLogLevel;
import com.jivesoftware.os.miru.plugin.solution.MiruTimeRange;
import com.jivesoftware.os.miru.reco.plugins.trending.TrendingAnswer;
import com.jivesoftware.os.miru.reco.plugins.trending.TrendingConstants;
import com.jivesoftware.os.miru.reco.plugins.trending.TrendingQuery;
import com.jivesoftware.os.miru.reco.plugins.trending.Trendy;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
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
        final List<String> fieldPrefixes;
        final String logLevel;

        public TrendingPluginRegionInput(String tenant,
            int fromHoursAgo,
            int toHoursAgo,
            int buckets,
            String field,
            List<String> fieldPrefixes,
            String logLevel) {
            this.tenant = tenant;
            this.fromHoursAgo = fromHoursAgo;
            this.toHoursAgo = toHoursAgo;
            this.buckets = buckets;
            this.field = field;
            this.fieldPrefixes = fieldPrefixes;
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
                data.put("fieldPrefixes", input.fieldPrefixes != null ? Joiner.on(", ").join(input.fieldPrefixes) : "");

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

                MiruFilter constraintsFilter = new MiruFilter(MiruFilterOperation.and, false, fieldFilters, null);

                List<RequestHelper> requestHelpers = readerRequestHelpers.get(Optional.<MiruHost>absent());
                MiruResponse<TrendingAnswer> response = null;
                if (!input.tenant.trim().isEmpty()) {
                    MiruTenantId tenantId = new MiruTenantId(input.tenant.trim().getBytes(Charsets.UTF_8));
                    for (RequestHelper requestHelper : requestHelpers) {
                        try {
                            @SuppressWarnings("unchecked")
                            MiruResponse<TrendingAnswer> trendingResponse = requestHelper.executeRequest(
                                new MiruRequest<>(tenantId, MiruActorId.NOT_PROVIDED, MiruAuthzExpression.NOT_PROVIDED,
                                    new TrendingQuery(
                                        TrendingQuery.Strategy.LINEAR_REGRESSION,
                                        new MiruTimeRange(fromTime, toTime),
                                        null,
                                        input.buckets,
                                        constraintsFilter,
                                        input.field,
                                        MiruFilter.NO_FILTER,
                                        input.fieldPrefixes,
                                        100),
                                    MiruSolutionLogLevel.valueOf(input.logLevel)),
                                TrendingConstants.TRENDING_PREFIX + TrendingConstants.CUSTOM_QUERY_ENDPOINT, MiruResponse.class,
                                new Class[]{TrendingAnswer.class},
                                null);
                            response = trendingResponse;
                            if (response != null && response.answer != null) {
                                break;
                            } else {
                                log.warn("Empty trending response from {}, trying another", requestHelper);
                            }
                        } catch (Exception e) {
                            log.warn("Failed trending request to {}, trying another", new Object[]{requestHelper}, e);
                        }
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

                    data.put("results", Lists.transform(results, new Function<Trendy, Map<String, String>>() {
                        @Override
                        public Map<String, String> apply(Trendy input) {
                            return ImmutableMap.of(
                                "name", input.distinctValue,
                                "rank", String.valueOf(input.rank),
                                "waveform", "data:image/png;base64," + new PNGWaveforms()
                                .hitsToBase64PNGWaveform(600, 128, 10,
                                    ImmutableMap.of(input.distinctValue, new AnalyticsAnswer.Waveform(input.waveform)),
                                    Optional.of(mmd)));
                        }
                    }));
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
