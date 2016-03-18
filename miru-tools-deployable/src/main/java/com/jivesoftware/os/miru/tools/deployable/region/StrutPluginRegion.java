package com.jivesoftware.os.miru.tools.deployable.region;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.google.common.base.Joiner;
import com.google.common.base.Optional;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.jivesoftware.os.jive.utils.ordered.id.JiveEpochTimestampProvider;
import com.jivesoftware.os.jive.utils.ordered.id.SnowflakeIdPacker;
import com.jivesoftware.os.miru.api.MiruActorId;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.api.query.filter.FilterStringUtil;
import com.jivesoftware.os.miru.api.query.filter.MiruAuthzExpression;
import com.jivesoftware.os.miru.api.query.filter.MiruFilter;
import com.jivesoftware.os.miru.api.query.filter.MiruValue;
import com.jivesoftware.os.miru.plugin.solution.MiruRequest;
import com.jivesoftware.os.miru.plugin.solution.MiruResponse;
import com.jivesoftware.os.miru.plugin.solution.MiruSolutionLogLevel;
import com.jivesoftware.os.miru.plugin.solution.MiruTimeRange;
import com.jivesoftware.os.miru.stream.plugins.catwalk.CatwalkQuery;
import com.jivesoftware.os.miru.stream.plugins.catwalk.FeatureScore;
import com.jivesoftware.os.miru.stream.plugins.strut.HotOrNot;
import com.jivesoftware.os.miru.stream.plugins.strut.HotOrNot.Hotness;
import com.jivesoftware.os.miru.stream.plugins.strut.StrutAnswer;
import com.jivesoftware.os.miru.stream.plugins.strut.StrutConstants;
import com.jivesoftware.os.miru.stream.plugins.strut.StrutQuery;
import com.jivesoftware.os.miru.stream.plugins.strut.StrutQuery.Strategy;
import com.jivesoftware.os.miru.ui.MiruPageRegion;
import com.jivesoftware.os.miru.ui.MiruSoyRenderer;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import com.jivesoftware.os.routing.bird.http.client.HttpResponse;
import com.jivesoftware.os.routing.bird.http.client.HttpResponseMapper;
import com.jivesoftware.os.routing.bird.http.client.RoundRobinStrategy;
import com.jivesoftware.os.routing.bird.http.client.TenantAwareHttpClient;
import com.jivesoftware.os.routing.bird.shared.ClientCall.ClientResponse;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 *
 */
public class StrutPluginRegion implements MiruPageRegion<Optional<StrutPluginRegion.StrutPluginRegionInput>> {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    private final String template;
    private final MiruSoyRenderer renderer;
    private final TenantAwareHttpClient<String> readerClient;
    private final ObjectMapper requestMapper;
    private final HttpResponseMapper responseMapper;
    private final FilterStringUtil filterStringUtil = new FilterStringUtil();

    public StrutPluginRegion(String template,
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

    public static class StrutPluginRegionInput {

        final String tenant;
        final long fromTimeAgo;
        final String fromTimeUnit;
        final long toTimeAgo;
        final String toTimeUnit;
        final String catwalkId;
        final String modelId;
        final String gatherField;
        final String gatherFilters;
        final String featureFields;
        final String featureFilters;
        final String constraintField;
        final String constraintFilters;
        final Strategy strategy;
        final int desiredNumberOfResults;
        final int desiredModelSize;
        final String logLevel;

        public StrutPluginRegionInput(String tenant, long fromTimeAgo, String fromTimeUnit, long toTimeAgo, String toTimeUnit, String catwalkId, String modelId,
            String gatherField, String gatherFilters, String featureFields, String featureFilters, String constraintField,
            String constraintFilters, Strategy strategy, int desiredNumberOfResults, int desiredModelSize, String logLevel) {
            this.tenant = tenant;
            this.fromTimeAgo = fromTimeAgo;
            this.fromTimeUnit = fromTimeUnit;
            this.toTimeAgo = toTimeAgo;
            this.toTimeUnit = toTimeUnit;
            this.catwalkId = catwalkId;
            this.modelId = modelId;
            this.gatherField = gatherField;
            this.gatherFilters = gatherFilters;
            this.featureFields = featureFields;
            this.featureFilters = featureFilters;
            this.constraintField = constraintField;
            this.constraintFilters = constraintFilters;
            this.strategy = strategy;
            this.desiredNumberOfResults = desiredNumberOfResults;
            this.desiredModelSize = desiredModelSize;
            this.logLevel = logLevel;
        }
    }

    @Override
    public String render(Optional<StrutPluginRegionInput> optionalInput) {
        Map<String, Object> data = Maps.newHashMap();
        try {
            if (optionalInput.isPresent()) {
                StrutPluginRegionInput input = optionalInput.get();

                // "user context, user activityType context, user activityType contextType"
                String[] featuresSplit = input.featureFields.split("\\s*,\\s*");
                String[][] featureFields = new String[featuresSplit.length][];
                for (int i = 0; i < featuresSplit.length; i++) {
                    String[] fields = featuresSplit[i].trim().split("\\s+");
                    featureFields[i] = new String[fields.length];
                    for (int j = 0; j < fields.length; j++) {
                        featureFields[i][j] = fields[j].trim();
                    }
                }

                TimeUnit fromTimeUnit = TimeUnit.valueOf(input.fromTimeUnit);
                long fromTimeAgo = input.fromTimeAgo;
                long fromMillisAgo = fromTimeUnit.toMillis(fromTimeAgo);

                TimeUnit toTimeUnit = TimeUnit.valueOf(input.toTimeUnit);
                long toTimeAgo = input.toTimeAgo;
                long toMillisAgo = toTimeUnit.toMillis(toTimeAgo);

                data.put("logLevel", input.logLevel);
                data.put("tenant", input.tenant);
                data.put("fromTimeAgo", String.valueOf(fromTimeAgo));
                data.put("fromTimeUnit", String.valueOf(fromTimeUnit));
                data.put("toTimeAgo", String.valueOf(toTimeAgo));
                data.put("toTimeUnit", String.valueOf(toTimeUnit));
                data.put("toTimeUnit", String.valueOf(toTimeUnit));
                data.put("catwalkId", input.catwalkId);
                data.put("modelId", input.modelId);
                data.put("gatherField", input.gatherField);
                data.put("gatherFilters", input.gatherFilters);
                data.put("featureFields", input.featureFields);
                data.put("featureFilters", input.featureFilters);
                data.put("constraintField", input.constraintField);
                data.put("constraintFilters", input.constraintFilters);
                data.put("strategy", input.strategy.name());
                data.put("desiredNumberOfResults", input.desiredNumberOfResults);
                data.put("desiredModelSize", input.desiredModelSize);

                SnowflakeIdPacker snowflakeIdPacker = new SnowflakeIdPacker();
                long jiveCurrentTime = new JiveEpochTimestampProvider().getTimestamp();
                final long packCurrentTime = snowflakeIdPacker.pack(jiveCurrentTime, 0, 0);
                final long fromTime, toTime;
                if (fromMillisAgo > toMillisAgo) {
                    fromTime = packCurrentTime - snowflakeIdPacker.pack(fromMillisAgo, 0, 0);
                    toTime = packCurrentTime - snowflakeIdPacker.pack(toMillisAgo, 0, 0);
                } else {
                    fromTime = packCurrentTime - snowflakeIdPacker.pack(toMillisAgo, 0, 0);
                    toTime = packCurrentTime - snowflakeIdPacker.pack(fromMillisAgo, 0, 0);
                }

                MiruResponse<StrutAnswer> response = null;
                if (!input.tenant.trim().isEmpty()) {
                    MiruTenantId tenantId = new MiruTenantId(input.tenant.trim().getBytes(StandardCharsets.UTF_8));

                    /*String[][] featureFields = new String[input.featureFields.size()][];
                    for (int i = 0; i < featureFields.length; i++) {
                        featureFields[i] = input.featureFields.toArray(new String[0]);
                    }*/
                    MiruFilter constraintFilter = filterStringUtil.parse(input.constraintFilters);

                    MiruFilter gatherFilter = filterStringUtil.parse(input.gatherFilters);
                    MiruFilter featureFilter = filterStringUtil.parse(input.featureFilters);
                    String endpoint = StrutConstants.STRUT_PREFIX + StrutConstants.CUSTOM_QUERY_ENDPOINT;

                    CatwalkQuery catwalkQuery = new CatwalkQuery(MiruTimeRange.ALL_TIME,
                        input.gatherField,
                        gatherFilter,
                        featureFields,
                        featureFilter,
                        input.desiredModelSize);

                    String request = requestMapper.writeValueAsString(new MiruRequest<>("toolsStrut",
                        tenantId,
                        MiruActorId.NOT_PROVIDED,
                        MiruAuthzExpression.NOT_PROVIDED,
                        new StrutQuery(
                            input.catwalkId,
                            input.modelId,
                            catwalkQuery,
                            new MiruTimeRange(fromTime, toTime),
                            input.constraintField,
                            constraintFilter,
                            input.strategy,
                            featureFields, // todo seperate from catwalkQuery
                            featureFilter, // todo seperate from catwalkQuery
                            input.desiredNumberOfResults,
                            true),
                        MiruSolutionLogLevel.valueOf(input.logLevel)));
                    MiruResponse<StrutAnswer> strutResponse = readerClient.call("",
                        new RoundRobinStrategy(),
                        "strutPluginRegion",
                        httpClient -> {
                            HttpResponse httpResponse = httpClient.postJson(endpoint, request, null);
                            @SuppressWarnings("unchecked")
                            MiruResponse<StrutAnswer> extractResponse = responseMapper.extractResultFromResponse(httpResponse,
                                MiruResponse.class,
                                new Class<?>[] { StrutAnswer.class },
                                null);
                            return new ClientResponse<>(extractResponse, true);
                        });
                    if (strutResponse != null && strutResponse.answer != null) {
                        response = strutResponse;
                    } else {
                        LOG.warn("Empty strut response from {}", tenantId);
                    }
                }

                if (response != null && response.answer != null) {
                    List<HotOrNot> hotOrNots = response.answer.results;
                    if (hotOrNots != null) {
                        Collections.sort(hotOrNots);
                        List<Map<String, Object>> results = new ArrayList<>();
                        StringBuilder buf = new StringBuilder();
                        for (HotOrNot hotOrNot : hotOrNots) {
                            List<String> features = Lists.newArrayList();
                            List<Hotness>[] featureTerms = hotOrNot.features;
                            for (int i = 0; i < featureTerms.length; i++) {
                                String[] fields = featureFields[i];
                                List<Hotness> feature = featureTerms[i];
                                if (feature != null) {
                                    Collections.sort(feature, (o1, o2) -> Float.compare(o2.score, o1.score)); // sort descending
                                    for (Hotness hotness : feature) {
                                        if (hotness.values.length != fields.length) {
                                            features.add("[unknown=" + hotness.score + "]");
                                        } else {
                                            buf.append('[');
                                            for (int j = 0; j < hotness.values.length; j++) {
                                                if (j > 0) {
                                                    buf.append(',');
                                                }
                                                buf.append(fields[j]).append(':').append(valueToString(hotness.values[j]));
                                            }
                                            buf.append('=').append(hotness.score).append("] ");
                                            features.add(buf.toString());
                                            buf.setLength(0);
                                        }
                                    }
                                }
                            }

                            Map<String, Object> r = new HashMap<>();
                            r.put("value", valueToString(hotOrNot.value));
                            r.put("score", String.valueOf(hotOrNot.score));
                            r.put("features", features);
                            results.add(r);
                        }

                        data.put("results", results);
                    }

                    data.put("elapse", String.valueOf(response.totalElapsed));

                    ObjectMapper mapper = new ObjectMapper();
                    mapper.enable(SerializationFeature.INDENT_OUTPUT);
                    data.put("summary", Joiner.on("\n").join(response.log) + "\n\n" + mapper.writeValueAsString(response.solutions));
                }

            }
        } catch (Exception e) {
            LOG.error("Unable to retrieve data", e);
        }

        return renderer.render(template, data);
    }

    private String valueToString(MiruValue value) {
        return Joiner.on(',').join(value.parts);
    }

    public static class ScoredFeature implements Comparable<ScoredFeature> {

        private final FeatureScore featureScore;
        private final float score;

        public ScoredFeature(FeatureScore featureScore) {
            this.featureScore = featureScore;
            this.score = (float) featureScore.numerator / featureScore.denominator;
        }

        @Override
        public int compareTo(ScoredFeature o) {
            int c = -Float.compare(score, o.score);
            if (c != 0) {
                return c;
            }
            c = Integer.compare(featureScore.termIds.length, o.featureScore.termIds.length);
            if (c != 0) {
                return c;
            }
            for (int j = 0; j < featureScore.termIds.length; j++) {
                c = featureScore.termIds[j].compareTo(o.featureScore.termIds[j]);
                if (c != 0) {
                    return c;
                }
            }
            return 0;
        }
    }

    @Override
    public String getTitle() {
        return "Strut";
    }

}
