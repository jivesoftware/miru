package com.jivesoftware.os.miru.sea.anomaly.deployable.region;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.google.common.base.Joiner;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.jivesoftware.os.jive.utils.ordered.id.JiveEpochTimestampProvider;
import com.jivesoftware.os.jive.utils.ordered.id.SnowflakeIdPacker;
import com.jivesoftware.os.miru.api.MiruActorId;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.api.field.MiruFieldType;
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
import com.jivesoftware.os.miru.reco.plugins.trending.TrendingAnswer;
import com.jivesoftware.os.miru.reco.plugins.trending.TrendingConstants;
import com.jivesoftware.os.miru.reco.plugins.trending.TrendingQuery;
import com.jivesoftware.os.miru.reco.plugins.trending.TrendingQuery.Strategy;
import com.jivesoftware.os.miru.reco.plugins.trending.TrendingQueryScoreSet;
import com.jivesoftware.os.miru.reco.plugins.trending.Trendy;
import com.jivesoftware.os.miru.sea.anomaly.deployable.SeaAnomalySchemaConstants;
import com.jivesoftware.os.miru.sea.anomaly.deployable.endpoints.MinMaxDouble;
import com.jivesoftware.os.miru.ui.MiruPageRegion;
import com.jivesoftware.os.miru.ui.MiruSoyRenderer;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import com.jivesoftware.os.routing.bird.http.client.HttpResponse;
import com.jivesoftware.os.routing.bird.http.client.HttpResponseMapper;
import com.jivesoftware.os.routing.bird.http.client.RoundRobinStrategy;
import com.jivesoftware.os.routing.bird.http.client.TenantAwareHttpClient;
import com.jivesoftware.os.routing.bird.shared.ClientCall.ClientResponse;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 *
 */
// soy.sea.anomaly.page.trendsPluginRegion
public class SeaAnomalyTrendsPluginRegion implements MiruPageRegion<Optional<SeaAnomalyTrendsPluginRegion.TrendingPluginRegionInput>> {

    private static final MetricLogger log = MetricLoggerFactory.getLogger();

    private final String template;
    private final MiruSoyRenderer renderer;
    private final TenantAwareHttpClient<String> readerClient;
    private final ObjectMapper requestMapper;
    private final HttpResponseMapper responseMapper;

    public SeaAnomalyTrendsPluginRegion(String template,
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

        final String type;
        final String service;

        public TrendingPluginRegionInput(String type, String service) {
            this.type = type;
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

                data.put("type", input.type);
                data.put("service", input.service);
                data.put("fromHoursAgo", fromHoursAgo);

                SnowflakeIdPacker snowflakeIdPacker = new SnowflakeIdPacker();
                long jiveCurrentTime = new JiveEpochTimestampProvider().getTimestamp();
                final long packCurrentTime = snowflakeIdPacker.pack(jiveCurrentTime, 0, 0);
                final long fromTime = packCurrentTime - snowflakeIdPacker.pack(TimeUnit.HOURS.toMillis(fromHoursAgo), 0, 0);
                final long toTime = packCurrentTime - snowflakeIdPacker.pack(TimeUnit.HOURS.toMillis(toHoursAgo), 0, 0);
                List<MiruFieldFilter> fieldFilters = Lists.newArrayList();
                fieldFilters.add(MiruFieldFilter.ofTerms(MiruFieldType.primary, "type", input.type));
                if (input.service != null) {
                    fieldFilters.add(MiruFieldFilter.ofTerms(MiruFieldType.primary, "service", input.service));
                }

                MiruFilter constraintsFilter = new MiruFilter(MiruFilterOperation.and, false, fieldFilters, null);

                MiruResponse<TrendingAnswer> response = null;
                MiruTenantId tenantId = SeaAnomalySchemaConstants.TENANT_ID;
                int numberOfBuckets = 30;
                try {
                    String endpoint = TrendingConstants.TRENDING_PREFIX + TrendingConstants.CUSTOM_QUERY_ENDPOINT;
                    String request = requestMapper.writeValueAsString(new MiruRequest<>("anomalyTrends",
                        tenantId,
                        MiruActorId.NOT_PROVIDED,
                        MiruAuthzExpression.NOT_PROVIDED,
                        new TrendingQuery(
                            Collections.singletonList(new TrendingQueryScoreSet(
                                "anomaly",
                                Collections.singleton(Strategy.LINEAR_REGRESSION),
                                new MiruTimeRange(fromTime, toTime),
                                numberOfBuckets,
                                100)),
                            constraintsFilter,
                            input.service != null ? "instance" : "service",
                            Collections.emptyList()),
                        MiruSolutionLogLevel.INFO));
                    MiruResponse<TrendingAnswer> trendingResponse = readerClient.call("",
                        new RoundRobinStrategy(),
                        "seaAnomalyTrends",
                        httpClient -> {
                            HttpResponse httpResponse = httpClient.postJson(endpoint, request, null);
                            @SuppressWarnings("unchecked")
                            MiruResponse<TrendingAnswer> extractResponse = responseMapper.extractResultFromResponse(httpResponse,
                                MiruResponse.class,
                                new Class[] { TrendingAnswer.class },
                                null);
                            return new ClientResponse<>(extractResponse, true);
                        });
                    if (trendingResponse != null && trendingResponse.answer != null) {
                        response = trendingResponse;
                    } else {
                        log.warn("Empty trending response from {}", tenantId);
                    }
                } catch (Exception x) {
                    log.warn("Failed to get a valid request helper.", x);
                }

                if (response != null && response.answer != null) {
                    data.put("elapse", String.valueOf(response.totalElapsed));

                    List<Waveform> answerWaveforms = response.answer.waveforms.get("anomaly");
                    Map<MiruValue, Waveform> waveforms = Maps.uniqueIndex(answerWaveforms, Waveform::getId);
                    List<Trendy> results = response.answer.scoreSets.get("anomaly").results.get(Strategy.LINEAR_REGRESSION.name());
                    if (results == null) {
                        results = Collections.emptyList();
                    }
                    data.put("elapse", String.valueOf(response.totalElapsed));
                    //data.put("waveform", waveform == null ? "" : waveform.toString());

                    final MinMaxDouble mmd = new MinMaxDouble();
                    mmd.value(0);
                    Map<String, long[]> pngWaveforms = Maps.newHashMap();
                    for (Trendy t : results) {
                        long[] waveform = new long[numberOfBuckets];
                        waveforms.get(t.distinctValue).mergeWaveform(waveform);
                        for (long w : waveform) {
                            mmd.value(w);
                        }
                        pngWaveforms.put(t.distinctValue.last(), waveform);
                    }

                    data.put("results", Lists.transform(results, trendy -> ImmutableMap.of(
                        "name", trendy.distinctValue,
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
        return "Anomalies";
    }
}
