package com.jivesoftware.os.miru.tools.deployable.region;

import com.google.common.base.Charsets;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.jivesoftware.os.jive.utils.ordered.id.JiveEpochTimestampProvider;
import com.jivesoftware.os.jive.utils.ordered.id.SnowflakeIdPacker;
import com.jivesoftware.os.miru.analytics.plugins.analytics.AnalyticsAnswer;
import com.jivesoftware.os.miru.analytics.plugins.analytics.AnalyticsConstants;
import com.jivesoftware.os.miru.analytics.plugins.analytics.AnalyticsQuery;
import com.jivesoftware.os.miru.analytics.plugins.analytics.AnalyticsQueryScoreSet;
import com.jivesoftware.os.miru.api.MiruActorId;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.api.field.MiruFieldType;
import com.jivesoftware.os.miru.api.query.filter.FilterStringUtil;
import com.jivesoftware.os.miru.api.query.filter.MiruAuthzExpression;
import com.jivesoftware.os.miru.api.query.filter.MiruFieldFilter;
import com.jivesoftware.os.miru.api.query.filter.MiruFilter;
import com.jivesoftware.os.miru.api.query.filter.MiruFilterOperation;
import com.jivesoftware.os.miru.plugin.query.MiruTenantQueryRouting;
import com.jivesoftware.os.miru.plugin.solution.MiruRequest;
import com.jivesoftware.os.miru.plugin.solution.MiruResponse;
import com.jivesoftware.os.miru.plugin.solution.MiruSolutionLogLevel;
import com.jivesoftware.os.miru.plugin.solution.MiruTimeRange;
import com.jivesoftware.os.miru.plugin.solution.Waveform;
import com.jivesoftware.os.miru.ui.MiruPageRegion;
import com.jivesoftware.os.miru.ui.MiruSoyRenderer;
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
// soy.miru.page.realwavePluginRegion
public class RealwavePluginRegion implements MiruPageRegion<Optional<RealwavePluginRegion.RealwavePluginRegionInput>> {

    private static final MetricLogger log = MetricLoggerFactory.getLogger();

    private final String template;
    private final MiruSoyRenderer renderer;
    private final MiruTenantQueryRouting miruTenantQueryRouting;
    private final FilterStringUtil filterStringUtil = new FilterStringUtil();

    public RealwavePluginRegion(String template,
        MiruSoyRenderer renderer,
        MiruTenantQueryRouting miruTenantQueryRouting) {
        this.template = template;
        this.renderer = renderer;
        this.miruTenantQueryRouting = miruTenantQueryRouting;
    }

    public static class RealwavePluginRegionInput {

        final String tenant;
        final long startTimestamp;
        final int lookbackSeconds;
        final int buckets;
        final String field1;
        final String terms1;
        final String field2;
        final String terms2;
        final String filters;
        final String graphType;
        final boolean legend;
        final int width;
        final int height;
        final boolean requireFocus;

        public RealwavePluginRegionInput(String tenant,
            long startTimestamp,
            int lookbackSeconds,
            int buckets,
            String field1,
            String terms1,
            String field2,
            String terms2,
            String filters,
            String graphType,
            boolean legend,
            int width,
            int height,
            boolean requireFocus) {
            this.tenant = tenant;
            this.startTimestamp = startTimestamp;
            this.lookbackSeconds = lookbackSeconds;
            this.buckets = buckets;
            this.field1 = field1;
            this.terms1 = terms1;
            this.field2 = field2;
            this.terms2 = terms2;
            this.filters = filters;
            this.graphType = graphType;
            this.legend = legend;
            this.width = width;
            this.height = height;
            this.requireFocus = requireFocus;
        }
    }

    @Override
    public String render(Optional<RealwavePluginRegionInput> optionalInput) {
        Map<String, Object> data = Maps.newHashMap();
        try {
            if (optionalInput.isPresent()) {
                RealwavePluginRegionInput input = optionalInput.get();

                data.put("tenant", input.tenant);
                data.put("lookbackSeconds", String.valueOf(input.lookbackSeconds));
                data.put("buckets", String.valueOf(input.buckets));
                data.put("field1", input.field1);
                data.put("terms1", input.terms1);
                data.put("field2", input.field2);
                data.put("terms2", input.terms2);
                data.put("filters", input.filters);
                data.put("graphType", input.graphType);

                boolean execute = !input.tenant.isEmpty()
                    && input.lookbackSeconds > 0
                    && input.buckets > 0
                    && !input.field1.isEmpty()
                    && !input.terms1.isEmpty();
                data.put("execute", execute);
            }
        } catch (Exception e) {
            log.error("Unable to retrieve data", e);
        }

        return renderer.render(template, data);
    }

    public Map<String, Object> poll(RealwavePluginRegionInput input) throws Exception {
        List<String> terms1 = Lists.newArrayList();
        for (String term : input.terms1.split(",")) {
            String trimmed = term.trim();
            if (!trimmed.isEmpty()) {
                terms1.add(trimmed);
            }
        }

        List<String> terms2 = Lists.newArrayList();
        for (String term : input.terms2.split(",")) {
            String trimmed = term.trim();
            if (!trimmed.isEmpty()) {
                terms2.add(trimmed);
            }
        }

        SnowflakeIdPacker snowflakeIdPacker = new SnowflakeIdPacker();
        // e.g. mpb=10,000, current=24,478, modulus=4,478, ceiling=30,000
        long millisPerBucket = TimeUnit.SECONDS.toMillis(input.lookbackSeconds) / input.buckets;
        long jiveCurrentTime = new JiveEpochTimestampProvider().getTimestamp();
        long jiveModulusTime = jiveCurrentTime % millisPerBucket;
        long jiveCeilingTime = jiveCurrentTime - jiveModulusTime + millisPerBucket;
        final long packCeilingTime = snowflakeIdPacker.pack(jiveCeilingTime, 0, 0);
        final long packLookbackTime = packCeilingTime - snowflakeIdPacker.pack(TimeUnit.SECONDS.toMillis(input.lookbackSeconds), 0, 0);

        MiruFilter constraintsFilter = filterStringUtil.parseFilters(input.filters);

        MiruResponse<AnalyticsAnswer> response = null;
        if (!input.tenant.trim().isEmpty()) {
            MiruTenantId tenantId = new MiruTenantId(input.tenant.trim().getBytes(Charsets.UTF_8));
            ImmutableMap.Builder<String, MiruFilter> analyticsFiltersBuilder = ImmutableMap.builder();
            for (String term1 : terms1) {
                if (input.field2.isEmpty() || terms2.isEmpty()) {
                    analyticsFiltersBuilder.put(
                        term1,
                        new MiruFilter(MiruFilterOperation.and,
                            false,
                            Collections.singletonList(
                                MiruFieldFilter.ofTerms(MiruFieldType.primary, input.field1, term1)),
                            null));
                } else {
                    for (String term2 : terms2) {
                        analyticsFiltersBuilder.put(
                            term1 + ", " + term2,
                            new MiruFilter(MiruFilterOperation.and,
                                false,
                                Arrays.asList(
                                    MiruFieldFilter.ofTerms(MiruFieldType.primary, input.field1, term1),
                                    MiruFieldFilter.ofTerms(MiruFieldType.primary, input.field2, term2)),
                                null));
                    }
                }
            }
            ImmutableMap<String, MiruFilter> analyticsFilters = analyticsFiltersBuilder.build();

            String endpoint = AnalyticsConstants.ANALYTICS_PREFIX + AnalyticsConstants.CUSTOM_QUERY_ENDPOINT;
            MiruRequest<AnalyticsQuery> miruRequest = new MiruRequest<>("toolsRealwave",
                tenantId,
                MiruActorId.NOT_PROVIDED,
                MiruAuthzExpression.NOT_PROVIDED,
                new AnalyticsQuery(
                    Collections.singletonList(new AnalyticsQueryScoreSet("tools",
                        new MiruTimeRange(packLookbackTime, packCeilingTime),
                        input.buckets)),
                    constraintsFilter,
                    analyticsFilters),
                MiruSolutionLogLevel.NONE);

            MiruResponse<AnalyticsAnswer> analyticsResponse = miruTenantQueryRouting.query("", "realwavePluginRegion",
                miruRequest, endpoint, AnalyticsAnswer.class);

            if (analyticsResponse != null && analyticsResponse.answer != null) {
                response = analyticsResponse;
            } else {
                log.warn("Empty analytics response from {}", tenantId);
            }
        }

        Map<String, Object> data = Maps.newHashMap();
        if (response != null && response.answer != null && response.answer.waveforms != null) {
            data.put("elapse", String.valueOf(response.totalElapsed));

            List<Waveform> waveforms = response.answer.waveforms.get("tools");
            if (waveforms == null) {
                waveforms = Collections.emptyList();
            }

            Map<String, Object> waveformData = Maps.newHashMap();
            long[] waveform = new long[input.buckets];
            for (Waveform entry : waveforms) {
                Arrays.fill(waveform, 0);
                entry.mergeWaveform(waveform);
                int[] counts = new int[waveform.length];
                for (int i = 0; i < counts.length; i++) {
                    counts[i] = (int) Math.min(waveform[i], Integer.MAX_VALUE);
                }
                waveformData.put(entry.getId().last(), counts);
            }
            data.put("waveforms", waveformData);
        }
        return data;
    }

    @Override
    public String getTitle() {
        return "Realwave";
    }

}
