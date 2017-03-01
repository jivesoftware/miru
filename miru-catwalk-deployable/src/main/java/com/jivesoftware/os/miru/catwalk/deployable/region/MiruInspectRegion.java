package com.jivesoftware.os.miru.catwalk.deployable.region;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.catwalk.deployable.CatwalkModelService;
import com.jivesoftware.os.miru.catwalk.deployable.CatwalkModelService.FeatureRange;
import com.jivesoftware.os.miru.catwalk.deployable.CatwalkModelService.MergedScores;
import com.jivesoftware.os.miru.catwalk.deployable.ModelFeatureScores;
import com.jivesoftware.os.miru.catwalk.deployable.region.MiruInspectRegion.InspectInput;
import com.jivesoftware.os.miru.catwalk.shared.CatwalkQuery.CatwalkFeature;
import com.jivesoftware.os.miru.catwalk.shared.FeatureScore;
import com.jivesoftware.os.miru.ui.MiruPageRegion;
import com.jivesoftware.os.miru.ui.MiruSoyRenderer;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeSet;

/**
 *
 */
// soy.miru.page.inspectRegion
public class MiruInspectRegion implements MiruPageRegion<InspectInput> {

    private static final MetricLogger log = MetricLoggerFactory.getLogger();

    private final String template;
    private final MiruSoyRenderer renderer;
    private final CatwalkModelService catwalkModelService;

    public MiruInspectRegion(String template,
        MiruSoyRenderer renderer,
        CatwalkModelService catwalkModelService) {
        this.template = template;
        this.renderer = renderer;
        this.catwalkModelService = catwalkModelService;
    }

    public static class InspectInput {

        public final String tenantId;
        public final String catwalkId;
        public final String modelId;
        public final String features;

        public InspectInput(String tenantId, String catwalkId, String modelId, String features) {
            this.tenantId = tenantId;
            this.catwalkId = catwalkId;
            this.modelId = modelId;
            this.features = features;
        }
    }

    @Override
    public String render(InspectInput input) {
        Map<String, Object> data = Maps.newHashMap();
        try {
            data.put("tenantId", input.tenantId != null ? input.tenantId : "");
            data.put("catwalkId", input.catwalkId != null ? input.catwalkId : "");
            data.put("modelId", input.modelId != null ? input.modelId : "");
            data.put("features", input.features != null ? input.features : "");

            if (input.tenantId != null && input.catwalkId != null && input.modelId != null && input.features != null) {
                MiruTenantId tenantId = new MiruTenantId(input.tenantId.trim().getBytes(StandardCharsets.UTF_8));
                String catwalkId = input.catwalkId.trim();
                String modelId = input.modelId.trim();
                String[] featureNames = input.features.trim().split("\\s*,\\s*");

                CatwalkFeature[] features = new CatwalkFeature[featureNames.length];
                for (int i = 0; i < features.length; i++) {
                    features[i] = new CatwalkFeature(featureNames[i], null, null); //TODO hacky
                }

                TreeSet<Integer> partitionIds = Sets.newTreeSet();
                List<FeatureRange> deletableRanges = Lists.newArrayList();
                Map<String, MergedScores> scores = catwalkModelService.gatherModel(tenantId, catwalkId, modelId, features, partitionIds, deletableRanges);

                List<Map<String, Object>> modelFeatures = Lists.newArrayList();
                for (Entry<String, MergedScores> entry : scores.entrySet()) {
                    ModelFeatureScores mergedScores = entry.getValue().mergedScores;
                    List<FeatureRange> ranges = entry.getValue().allRanges;
                    StringBuilder rangeData = new StringBuilder();
                    for (FeatureRange range : ranges) {
                        if (rangeData.length() > 0) {
                            rangeData.append(", ");
                        }
                        rangeData.append(range.fromPartitionId).append('-').append(range.toPartitionId);
                    }

                    List<Map<String, Object>> scoreData = Lists.newArrayList();
                    for (FeatureScore featureScore : mergedScores.featureScores) {
                        float[] s = new float[featureScore.numerators.length];
                        for (int i = 0; i < s.length; i++) {
                            s[i] = (float) featureScore.numerators[i] / featureScore.denominator;
                        }
                        scoreData.add(ImmutableMap.of(
                            "value", Arrays.toString(featureScore.termIds),
                            "numerator", Arrays.toString(featureScore.numerators),
                            "denominator", String.valueOf(featureScore.denominator),
                            "numPartitions", String.valueOf(featureScore.numPartitions),
                            "score", Arrays.toString(s)));
                    }

                    modelFeatures.add(ImmutableMap.<String, Object>builder()
                        .put("name", entry.getKey())
                        .put("modelCount", String.valueOf(mergedScores.modelCount))
                        .put("totalCount", String.valueOf(mergedScores.totalCount))
                        .put("ranges", rangeData.toString())
                        .put("scores", scoreData)
                        .build());
                }

                data.put("model", ImmutableMap.of(
                    "features", modelFeatures));
            }
        } catch (Exception e) {
            log.error("Unable to retrieve data");
        }

        return renderer.render(template, data);
    }

    @Override
    public String getTitle() {
        return "Catwalk - Inspect";
    }
}
