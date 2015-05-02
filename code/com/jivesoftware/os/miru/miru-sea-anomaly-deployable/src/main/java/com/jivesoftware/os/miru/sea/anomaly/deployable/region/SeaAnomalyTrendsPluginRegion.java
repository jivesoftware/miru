package com.jivesoftware.os.miru.sea.anomaly.deployable.region;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.google.common.base.Joiner;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.jivesoftware.os.jive.utils.http.client.rest.RequestHelper;
import com.jivesoftware.os.jive.utils.ordered.id.JiveEpochTimestampProvider;
import com.jivesoftware.os.jive.utils.ordered.id.SnowflakeIdPacker;
import com.jivesoftware.os.miru.api.MiruActorId;
import com.jivesoftware.os.miru.api.MiruHost;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.api.field.MiruFieldType;
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
import com.jivesoftware.os.miru.sea.anomaly.deployable.MiruSoyRenderer;
import com.jivesoftware.os.miru.sea.anomaly.deployable.SeaAnomalySchemaConstants;
import com.jivesoftware.os.miru.sea.anomaly.deployable.endpoints.MinMaxDouble;
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
// soy.sea.anomaly.page.trendsPluginRegion
public class SeaAnomalyTrendsPluginRegion implements PageRegion<Optional<SeaAnomalyTrendsPluginRegion.TrendingPluginRegionInput>> {

    private static final MetricLogger log = MetricLoggerFactory.getLogger();

    private final String template;
    private final MiruSoyRenderer renderer;
    private final ReaderRequestHelpers miruReaders;

    public SeaAnomalyTrendsPluginRegion(String template,
        MiruSoyRenderer renderer,
        ReaderRequestHelpers miruReaders) {
        this.template = template;
        this.renderer = renderer;
        this.miruReaders = miruReaders;
    }

    public static class TrendingPluginRegionInput {

        final String logLevel;
        final String service;

        public TrendingPluginRegionInput(String logLevel, String service) {
            this.logLevel = logLevel;
            this.service = service;
        }
    }

    @Override
    public String render(Optional<TrendingPluginRegionInput> optionalInput) {
        Map<String, Object> data = Maps.newHashMap();
        try {
            if (optionalInput.isPresent()) {
                TrendingPluginRegionInput input = optionalInput.get();
                int fromHoursAgo = 8;
                int toHoursAgo = 0;

                data.put("logLevel", input.logLevel);
                data.put("service", input.service);
                data.put("fromHoursAgo", fromHoursAgo);

                SnowflakeIdPacker snowflakeIdPacker = new SnowflakeIdPacker();
                long jiveCurrentTime = new JiveEpochTimestampProvider().getTimestamp();
                final long packCurrentTime = snowflakeIdPacker.pack(jiveCurrentTime, 0, 0);
                final long fromTime = packCurrentTime - snowflakeIdPacker.pack(TimeUnit.HOURS.toMillis(fromHoursAgo), 0, 0);
                final long toTime = packCurrentTime - snowflakeIdPacker.pack(TimeUnit.HOURS.toMillis(toHoursAgo), 0, 0);
                List<MiruFieldFilter> fieldFilters = Lists.newArrayList();
                fieldFilters.add(new MiruFieldFilter(MiruFieldType.primary, "level", Arrays.asList(String.valueOf(input.logLevel))));
                if (input.service != null) {
                    fieldFilters.add(new MiruFieldFilter(MiruFieldType.primary, "service", Arrays.asList(input.service)));
                }

                MiruFilter constraintsFilter = new MiruFilter(MiruFilterOperation.and, false, fieldFilters, null);

                MiruResponse<TrendingAnswer> response = null;
                MiruTenantId tenantId = SeaAnomalySchemaConstants.TENANT_ID;
                try {
                    for (RequestHelper requestHelper : miruReaders.get(Optional.<MiruHost>absent())) {
                        try {
                            @SuppressWarnings("unchecked")
                            MiruResponse<TrendingAnswer> trendingResponse = requestHelper.executeRequest(
                                new MiruRequest<>(tenantId,
                                    MiruActorId.NOT_PROVIDED,
                                    MiruAuthzExpression.NOT_PROVIDED,
                                    new TrendingQuery(TrendingQuery.Strategy.LINEAR_REGRESSION,
                                        new MiruTimeRange(fromTime, toTime),
                                        null,
                                        30,
                                        constraintsFilter,
                                        input.service != null ? "instance" : "service",
                                        MiruFilter.NO_FILTER,
                                        null,
                                        100),
                                    MiruSolutionLogLevel.INFO),
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
                } catch (Exception x) {
                    log.warn("Failed to get a valid request helper.", x);
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
        return "Anomalies";
    }
}
