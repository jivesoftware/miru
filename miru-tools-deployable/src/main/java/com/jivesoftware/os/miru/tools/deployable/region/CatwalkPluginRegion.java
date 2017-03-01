package com.jivesoftware.os.miru.tools.deployable.region;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.google.common.base.Joiner;
import com.google.common.base.Optional;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.primitives.Floats;
import com.jivesoftware.os.jive.utils.ordered.id.JiveEpochTimestampProvider;
import com.jivesoftware.os.jive.utils.ordered.id.SnowflakeIdPacker;
import com.jivesoftware.os.miru.api.MiruActorId;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.api.query.filter.FilterStringUtil;
import com.jivesoftware.os.miru.api.query.filter.MiruAuthzExpression;
import com.jivesoftware.os.miru.api.query.filter.MiruFilter;
import com.jivesoftware.os.miru.plugin.solution.MiruRequest;
import com.jivesoftware.os.miru.plugin.solution.MiruResponse;
import com.jivesoftware.os.miru.plugin.solution.MiruSolutionLogLevel;
import com.jivesoftware.os.miru.plugin.solution.MiruTimeRange;
import com.jivesoftware.os.miru.stream.plugins.catwalk.CatwalkAnswer;
import com.jivesoftware.os.miru.stream.plugins.catwalk.CatwalkConstants;
import com.jivesoftware.os.miru.catwalk.shared.CatwalkQuery;
import com.jivesoftware.os.miru.catwalk.shared.CatwalkQuery.CatwalkFeature;
import com.jivesoftware.os.miru.catwalk.shared.FeatureScore;
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
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 *
 */
// soy.miru.page.analyticsPluginRegion
public class CatwalkPluginRegion implements MiruPageRegion<Optional<CatwalkPluginRegion.CatwalkPluginRegionInput>> {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    private final String template;
    private final MiruSoyRenderer renderer;
    private final TenantAwareHttpClient<String> readerClient;
    private final ObjectMapper requestMapper;
    private final HttpResponseMapper responseMapper;
    private final FilterStringUtil filterStringUtil = new FilterStringUtil();

    public CatwalkPluginRegion(String template,
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

    public static class CatwalkPluginRegionInput {

        final String tenant;
        final long fromTimeAgo;
        final String fromTimeUnit;
        final long toTimeAgo;
        final String toTimeUnit;
        final String catwalkId;
        final String scorableField;
        final String numeratorFilters;
        /*final String featureFields;
        final String featureFilters;*/
        final String features;
        final int desiredNumberOfResults;
        final String logLevel;

        public CatwalkPluginRegionInput(String tenant,
            long fromTimeAgo,
            String fromTimeUnit,
            long toTimeAgo,
            String toTimeUnit,
            String catwalkId,
            String scorableField,
            String numeratorFilters,
            /*String featureFields,
            String featureFilters,*/
            String features,
            int desiredNumberOfResults,
            String logLevel) {
            this.tenant = tenant;
            this.fromTimeAgo = fromTimeAgo;
            this.fromTimeUnit = fromTimeUnit;
            this.toTimeAgo = toTimeAgo;
            this.toTimeUnit = toTimeUnit;
            this.catwalkId = catwalkId;
            this.scorableField = scorableField;
            this.numeratorFilters = numeratorFilters;
            /*this.featureFields = featureFields;
            this.featureFilters = featureFilters;*/
            this.features = features;
            this.desiredNumberOfResults = desiredNumberOfResults;
            this.logLevel = logLevel;
        }
    }

    @Override
    public String render(Optional<CatwalkPluginRegionInput> optionalInput) {
        Map<String, Object> data = Maps.newHashMap();
        try {
            if (optionalInput.isPresent()) {
                CatwalkPluginRegionInput input = optionalInput.get();

                // name1; field1, field2; field1:a|b|c, field2:x|y|z
                // name2; field1, field3; field1:a|b|c, field3:i|j|k
                // name3; field2, field3; field2:x|y|z, field3:i|j|k
                String[] featureSplit = input.features.split("\\s*\\n\\s*");
                List<CatwalkFeature> features = Lists.newArrayList();
                String[] featureNames = new String[featureSplit.length];
                for (int i = 0; i < featureSplit.length; i++) {
                    String[] featureParts = featureSplit[i].split("\\s*;\\s*");
                    if (featureParts.length == 3) {
                        featureNames[i] = featureParts[0].trim();
                        String[] featureFields = featureParts[1].split("\\s*,\\s*");
                        MiruFilter featureFilter = filterStringUtil.parseFilters(featureParts[2]);
                        features.add(new CatwalkFeature(featureNames[i], featureFields, featureFilter));
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
                data.put("scorableField", input.scorableField);
                data.put("numeratorFilters", input.numeratorFilters);
                /*data.put("featureFields", input.featureFields);
                data.put("featureFilters", input.featureFilters);*/
                data.put("features", input.features);
                data.put("desiredNumberOfResults", input.desiredNumberOfResults);

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

                MiruResponse<CatwalkAnswer> response = null;
                if (!input.tenant.trim().isEmpty()) {
                    MiruTenantId tenantId = new MiruTenantId(input.tenant.trim().getBytes(StandardCharsets.UTF_8));

                    String[] numeratorFiltersSplit = input.numeratorFilters.split("\\s*\\n\\s*");
                    List<MiruFilter> numeratorFilters = Lists.newArrayList();
                    for (int i = 0; i < numeratorFiltersSplit.length; i++) {
                        String filterString = numeratorFiltersSplit[i].trim();
                        if (!filterString.isEmpty()) {
                            numeratorFilters.add(filterStringUtil.parseFilters(filterString));
                        }
                    }
                    String endpoint = CatwalkConstants.CATWALK_PREFIX + CatwalkConstants.CUSTOM_QUERY_ENDPOINT;
                    String request = requestMapper.writeValueAsString(new MiruRequest<>("toolsCatwalk",
                        tenantId,
                        MiruActorId.NOT_PROVIDED,
                        MiruAuthzExpression.NOT_PROVIDED,
                        new CatwalkQuery(
                            input.catwalkId,
                            new MiruTimeRange(fromTime, toTime),
                            input.scorableField,
                            numeratorFilters.toArray(new MiruFilter[0]),
                            features.toArray(new CatwalkFeature[0]),
                            input.desiredNumberOfResults),
                        MiruSolutionLogLevel.valueOf(input.logLevel)));
                    MiruResponse<CatwalkAnswer> catwalkResponse = readerClient.call("",
                        new RoundRobinStrategy(),
                        "catwalkPluginRegion",
                        httpClient -> {
                            HttpResponse httpResponse = httpClient.postJson(endpoint, request, null);
                            @SuppressWarnings("unchecked")
                            MiruResponse<CatwalkAnswer> extractResponse = responseMapper.extractResultFromResponse(httpResponse,
                                MiruResponse.class,
                                new Class<?>[] { CatwalkAnswer.class },
                                null);
                            return new ClientResponse<>(extractResponse, true);
                        });
                    if (catwalkResponse != null && catwalkResponse.answer != null) {
                        response = catwalkResponse;
                    } else {
                        LOG.warn("Empty catwalk response from {}", tenantId);
                    }
                }

                if (response != null && response.answer != null) {
                    List<FeatureScore>[] results = response.answer.results;
                    if (results != null) {
                        List<Map<String, Object>> featureClasses = new ArrayList<>();
                        for (int i = 0; i < results.length; i++) {
                            List<FeatureScore> result = results[i];
                            List<ScoredFeature> scored = Lists.newArrayList();
                            for (FeatureScore r : result) {
                                scored.add(new ScoredFeature(r));
                            }
                            Collections.sort(scored);

                            List<Map<String, Object>> modelFeatures = new ArrayList<>();
                            for (ScoredFeature scoredFeature : scored) {
                                Map<String, Object> feature = new HashMap<>();
                                List<String> values = Lists.transform(Arrays.asList(scoredFeature.featureScore.termIds),
                                    (input1) -> new String(input1.getBytes(), StandardCharsets.UTF_8));
                                feature.put("values", values);
                                feature.put("numerator", Arrays.toString(scoredFeature.featureScore.numerators));
                                feature.put("denominator", String.valueOf(scoredFeature.featureScore.denominator));
                                feature.put("score", Arrays.toString(scoredFeature.scores));
                                modelFeatures.add(feature);
                            }

                            CatwalkFeature feature = features.get(i);
                            String name = Joiner.on(',').join(feature.featureFields);
                            Map<String, Object> featureClass = new HashMap<>();
                            featureClass.put("name", feature.name + ":" + name);
                            featureClass.put("nameParts", Arrays.asList(feature.featureFields));
                            featureClass.put("modelFeatures", modelFeatures);

                            featureClasses.add(featureClass);
                        }

                        HashMap<String, Object> model = new HashMap<>();
                        model.put("modelCounts", Arrays.toString(response.answer.modelCounts));
                        model.put("totalCount", String.valueOf(response.answer.totalCount));
                        model.put("featureClasses", featureClasses);
                        data.put("model", model);
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

    public static class ScoredFeature implements Comparable<ScoredFeature> {

        private final FeatureScore featureScore;
        private final float[] scores;

        public ScoredFeature(FeatureScore featureScore) {
            this.featureScore = featureScore;
            this.scores = new float[featureScore.numerators.length];
            for (int i = 0; i < this.scores.length; i++) {
                this.scores[i] = (float) featureScore.numerators[i] / featureScore.denominator;
            }
        }

        @Override
        public int compareTo(ScoredFeature o) {
            int c = -Float.compare(Floats.max(scores), Floats.max(o.scores));
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
        return "Catwalk";
    }

}
