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
import com.jivesoftware.os.miru.api.base.MiruStreamId;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.api.query.filter.FilterStringUtil;
import com.jivesoftware.os.miru.api.query.filter.MiruAuthzExpression;
import com.jivesoftware.os.miru.api.query.filter.MiruFilter;
import com.jivesoftware.os.miru.api.query.filter.MiruValue;
import com.jivesoftware.os.miru.catwalk.shared.CatwalkQuery.CatwalkDefinition;
import com.jivesoftware.os.miru.catwalk.shared.CatwalkQuery.CatwalkFeature;
import com.jivesoftware.os.miru.catwalk.shared.CatwalkQuery.CatwalkModelQuery;
import com.jivesoftware.os.miru.catwalk.shared.HotOrNot;
import com.jivesoftware.os.miru.catwalk.shared.HotOrNot.Hotness;
import com.jivesoftware.os.miru.catwalk.shared.Strategy;
import com.jivesoftware.os.miru.catwalk.shared.StrutModelScalar;
import com.jivesoftware.os.miru.plugin.query.MiruRouting;
import com.jivesoftware.os.miru.plugin.solution.MiruRequest;
import com.jivesoftware.os.miru.plugin.solution.MiruResponse;
import com.jivesoftware.os.miru.plugin.solution.MiruSolutionLogLevel;
import com.jivesoftware.os.miru.plugin.solution.MiruTimeRange;
import com.jivesoftware.os.miru.stream.plugins.strut.StrutAnswer;
import com.jivesoftware.os.miru.stream.plugins.strut.StrutConstants;
import com.jivesoftware.os.miru.stream.plugins.strut.StrutQuery;
import com.jivesoftware.os.miru.ui.MiruPageRegion;
import com.jivesoftware.os.miru.ui.MiruSoyRenderer;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
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
public class StrutPluginRegion implements MiruPageRegion<Optional<StrutPluginRegion.StrutPluginRegionInput>> {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    private final String template;
    private final MiruSoyRenderer renderer;
    private final MiruRouting routing;
    private final FilterStringUtil filterStringUtil;

    public StrutPluginRegion(String template,
        MiruSoyRenderer renderer,
        MiruRouting routing,
        FilterStringUtil filterStringUtil) {
        this.template = template;
        this.renderer = renderer;
        this.routing = routing;
        this.filterStringUtil = filterStringUtil;
    }

    public static class StrutPluginRegionInput {

        final String tenant;
        final long fromTimeAgo;
        final String fromTimeUnit;
        final long toTimeAgo;
        final String toTimeUnit;
        final String catwalkId;
        final String modelId;
        final String unreadStreamId;
        final boolean unreadOnly;
        final String gatherField;
        final String numeratorFilters;
        final String gatherTermsForFields;
        /*final String featureFields;
        final String featureFilters;*/
        final String features;
        final String scorableField;
        final String constraintFilters;
        final Strategy numeratorStrategy;
        final Strategy featureStrategy;
        final boolean usePartitionModelCache;
        final int desiredNumberOfResults;
        final int desiredModelSize;
        final String logLevel;

        public StrutPluginRegionInput(String tenant,
            long fromTimeAgo,
            String fromTimeUnit,
            long toTimeAgo,
            String toTimeUnit,
            String catwalkId,
            String modelId,
            String unreadStreamId,
            boolean unreadOnly,
            String gatherField,
            String numeratorFilters,
            String gatherTermsForFields,
            /*String featureFields,
            String featureFilters,*/
            String features,
            String scorableField,
            String constraintFilters,
            Strategy numeratorStrategy,
            Strategy featureStrategy,
            boolean usePartitionModelCache,
            int desiredNumberOfResults,
            int desiredModelSize,
            String logLevel) {
            this.tenant = tenant;
            this.fromTimeAgo = fromTimeAgo;
            this.fromTimeUnit = fromTimeUnit;
            this.toTimeAgo = toTimeAgo;
            this.toTimeUnit = toTimeUnit;
            this.catwalkId = catwalkId;
            this.modelId = modelId;
            this.unreadStreamId = unreadStreamId;
            this.unreadOnly = unreadOnly;
            this.gatherField = gatherField;
            this.numeratorFilters = numeratorFilters;
            this.gatherTermsForFields = gatherTermsForFields;
            /*this.featureFields = featureFields;
            this.featureFilters = featureFilters;*/
            this.features = features;
            this.scorableField = scorableField;
            this.constraintFilters = constraintFilters;
            this.numeratorStrategy = numeratorStrategy;
            this.featureStrategy = featureStrategy;
            this.usePartitionModelCache = usePartitionModelCache;
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

                // name1; field1, field2; field1:a|b|c, field2:x|y|z
                // name2; field1, field3; field1:a|b|c, field3:i|j|k
                // name3; field2, field3; field2:x|y|z, field3:i|j|k
                String[] featureSplit = input.features.split("\\s*\\n\\s*");
                List<CatwalkFeature> features = Lists.newArrayList();
                for (int i = 0; i < featureSplit.length; i++) {
                    String[] featureParts = featureSplit[i].split("\\s*;\\s*");
                    if (featureParts.length == 3) {
                        String featureName = featureParts[0].trim();
                        String[] featureFields = featureParts[1].split("\\s*,\\s*");
                        MiruFilter featureFilter = filterStringUtil.parseFilters(featureParts[2]);
                        features.add(new CatwalkFeature(featureName, featureFields, featureFilter, 1f));
                    }
                }

                String[] numeratorFiltersSplit = input.numeratorFilters.split("\\s*\\n\\s*");
                List<MiruFilter> numeratorFilters = Lists.newArrayList();
                for (int i = 0; i < numeratorFiltersSplit.length; i++) {
                    String filterString = numeratorFiltersSplit[i].trim();
                    if (!filterString.isEmpty()) {
                        numeratorFilters.add(filterStringUtil.parseFilters(filterString));
                    }
                }

                float[] numeratorScalars = new float[numeratorFilters.size()];
                Arrays.fill(numeratorScalars, 1f); //TODO

                // "user context, user activityType context, user activityType contextType"
                /*String[] featuresSplit = input.featureFields.split("\\s*,\\s*");
                String[][] featureFields = new String[featuresSplit.length][];
                for (int i = 0; i < featuresSplit.length; i++) {
                    String[] fields = featuresSplit[i].trim().split("\\s+");
                    featureFields[i] = new String[fields.length];
                    for (int j = 0; j < fields.length; j++) {
                        featureFields[i][j] = fields[j].trim();
                    }
                }*/
                String[] gatherTermsForFieldSplit = (input.gatherTermsForFields.isEmpty()) ? null : input.gatherTermsForFields.split("\\s*,\\s*");

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
                data.put("numeratorFilters", input.numeratorFilters);
                data.put("unreadStreamId", input.unreadStreamId);
                data.put("unreadOnly", input.unreadOnly);
                data.put("gatherTermsForFields", input.gatherTermsForFields);
                /*data.put("featureFields", input.featureFields);
                data.put("featureFilters", input.featureFilters);*/
                data.put("features", input.features);
                data.put("gatherField", input.scorableField);
                data.put("constraintFilters", input.constraintFilters);
                data.put("numeratorStrategy", input.numeratorStrategy.name());
                data.put("featureStrategy", input.featureStrategy.name());
                data.put("usePartitionModelCache", input.usePartitionModelCache);
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

                    MiruFilter constraintFilter = filterStringUtil.parseFilters(input.constraintFilters);

                    String endpoint = StrutConstants.STRUT_PREFIX + StrutConstants.CUSTOM_QUERY_ENDPOINT;

                    CatwalkDefinition catwalkDefinition = new CatwalkDefinition(input.catwalkId,
                        input.gatherField,
                        input.scorableField,
                        features.toArray(new CatwalkFeature[0]),
                        input.featureStrategy,
                        MiruFilter.NO_FILTER,
                        numeratorFilters.size());
                    CatwalkModelQuery modelQuery = new CatwalkModelQuery(
                        MiruTimeRange.ALL_TIME,
                        numeratorFilters.toArray(new MiruFilter[0]),
                        input.desiredModelSize);

                    MiruRequest<StrutQuery> miruRequest = new MiruRequest<>("toolsStrut",
                        tenantId,
                        MiruActorId.NOT_PROVIDED,
                        MiruAuthzExpression.NOT_PROVIDED,
                        new StrutQuery(
                            catwalkDefinition,
                            Collections.singletonList(new StrutModelScalar(input.modelId, modelQuery, 1f)),
                            new MiruTimeRange(fromTime, toTime),
                            constraintFilter,
                            input.numeratorStrategy,
                            numeratorScalars,
                            input.desiredNumberOfResults,
                            true,
                            gatherTermsForFieldSplit,
                            input.unreadStreamId.isEmpty() ? MiruStreamId.NULL : new MiruStreamId(input.unreadStreamId.getBytes(StandardCharsets.UTF_8)),
                            null, //TODO this prevents backfill
                            input.unreadOnly,
                            false,
                            100), // TODO expose to UI??
                        MiruSolutionLogLevel.valueOf(input.logLevel));

                    MiruResponse<StrutAnswer> strutResponse = routing.query("", "strutPluginRegion",
                        miruRequest, endpoint, StrutAnswer.class);

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
                            List<String> hotFeatures = Lists.newArrayList();
                            List<Hotness>[] featureTerms = hotOrNot.features;
                            if (featureTerms != null) {
                                for (int i = 0; i < featureTerms.length; i++) {
                                    String[] fields = features.get(i).featureFields;
                                    List<Hotness> feature = featureTerms[i];
                                    if (feature != null) {
                                        hotFeatures.add("<b>" + features.get(i).name + "</b>");

                                        // sort descending
                                        Collections.sort(feature, (o1, o2) -> Float.compare(Floats.max(o2.scores), Floats.max(o1.scores)));
                                        for (Hotness hotness : feature) {
                                            if (hotness.values.length != fields.length) {
                                                hotFeatures.add("[unknown=" + Arrays.toString(hotness.scores) + "]");
                                            } else {
                                                buf.append('[');
                                                for (int j = 0; j < hotness.values.length; j++) {
                                                    if (j > 0) {
                                                        buf.append(',');
                                                    }
                                                    buf.append(fields[j]).append(':').append(valueToString(hotness.values[j]));
                                                }
                                                buf.append('=').append(Arrays.toString(hotness.scores)).append("] ");
                                                hotFeatures.add(buf.toString());
                                                buf.setLength(0);
                                            }
                                        }
                                    }
                                }
                            }

                            long hotTime = snowflakeIdPacker.unpack(hotOrNot.timestamp)[0];
                            long millisAgo = jiveCurrentTime - hotTime;

                            Map<String, Object> r = new HashMap<>();
                            r.put("value", valueToString(hotOrNot.value));
                            r.put("score", String.valueOf(hotOrNot.score));
                            r.put("terms", (hotOrNot.gatherLatestValues == null) ? "" : Arrays.deepToString(hotOrNot.gatherLatestValues));
                            r.put("timeAgo", timeAgo(millisAgo));
                            r.put("features", hotFeatures);
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

    private static String timeAgo(long millis) {
        String suffix;
        if (millis >= 0) {
            suffix = "ago";
        } else {
            suffix = "from now";
            millis = Math.abs(millis);
        }

        final long hr = TimeUnit.MILLISECONDS.toHours(millis);
        final long min = TimeUnit.MILLISECONDS.toMinutes(millis - TimeUnit.HOURS.toMillis(hr));
        final long sec = TimeUnit.MILLISECONDS.toSeconds(millis - TimeUnit.HOURS.toMillis(hr) - TimeUnit.MINUTES.toMillis(min));
        final long ms = TimeUnit.MILLISECONDS.toMillis(millis - TimeUnit.HOURS.toMillis(hr) - TimeUnit.MINUTES.toMillis(min) - TimeUnit.SECONDS.toMillis(sec));
        return String.format("%02d:%02d:%02d.%03d " + suffix, hr, min, sec, ms);
    }

    @Override
    public String getTitle() {
        return "Strut";
    }

}
