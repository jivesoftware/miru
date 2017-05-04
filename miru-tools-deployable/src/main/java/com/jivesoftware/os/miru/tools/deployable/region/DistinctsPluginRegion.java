package com.jivesoftware.os.miru.tools.deployable.region;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.google.common.base.Charsets;
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
import com.jivesoftware.os.miru.plugin.query.MiruRouting;
import com.jivesoftware.os.miru.plugin.solution.MiruRequest;
import com.jivesoftware.os.miru.plugin.solution.MiruResponse;
import com.jivesoftware.os.miru.plugin.solution.MiruSolutionLogLevel;
import com.jivesoftware.os.miru.plugin.solution.MiruTimeRange;
import com.jivesoftware.os.miru.reco.plugins.distincts.DistinctsAnswer;
import com.jivesoftware.os.miru.reco.plugins.distincts.DistinctsConstants;
import com.jivesoftware.os.miru.reco.plugins.distincts.DistinctsQuery;
import com.jivesoftware.os.miru.ui.MiruPageRegion;
import com.jivesoftware.os.miru.ui.MiruSoyRenderer;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 *
 */
// soy.miru.page.distinctsPluginRegion
public class DistinctsPluginRegion implements MiruPageRegion<Optional<DistinctsPluginRegion.DistinctsPluginRegionInput>> {

    private static final MetricLogger log = MetricLoggerFactory.getLogger();

    private final String template;
    private final MiruSoyRenderer renderer;
    private final MiruRouting routing;
    private final FilterStringUtil filterStringUtil = new FilterStringUtil();

    public DistinctsPluginRegion(String template,
        MiruSoyRenderer renderer,
        MiruRouting routing) {
        this.template = template;
        this.renderer = renderer;
        this.routing = routing;
    }

    public static class DistinctsPluginRegionInput {

        final String tenant;
        final int fromHoursAgo;
        final int toHoursAgo;
        final String field;
        final String types;
        final String filters;
        final int maxCount;
        final String logLevel;

        public DistinctsPluginRegionInput(String tenant,
            int fromHoursAgo,
            int toHoursAgo,
            String field,
            String types,
            String filters,
            int maxCount,
            String logLevel) {

            this.tenant = tenant;
            this.fromHoursAgo = fromHoursAgo;
            this.toHoursAgo = toHoursAgo;
            this.field = field;
            this.types = types;
            this.filters = filters;
            this.maxCount = maxCount;
            this.logLevel = logLevel;
        }
    }

    @Override
    public String render(Optional<DistinctsPluginRegionInput> optionalInput) {
        Map<String, Object> data = Maps.newHashMap();
        try {
            if (optionalInput.isPresent()) {
                DistinctsPluginRegionInput input = optionalInput.get();
                int fromHoursAgo = input.fromHoursAgo > input.toHoursAgo ? input.fromHoursAgo : input.toHoursAgo;
                int toHoursAgo = input.fromHoursAgo > input.toHoursAgo ? input.toHoursAgo : input.fromHoursAgo;

                data.put("logLevel", input.logLevel);
                data.put("tenant", input.tenant);
                data.put("fromHoursAgo", String.valueOf(fromHoursAgo));
                data.put("toHoursAgo", String.valueOf(toHoursAgo));
                data.put("field", input.field);
                data.put("types", input.types);
                data.put("maxCount", input.maxCount);
                data.put("filters", input.filters);

                List<MiruValue> fieldTypes = filterStringUtil.buildFieldPrefixes(Arrays.asList(input.types.split(",")));

                SnowflakeIdPacker snowflakeIdPacker = new SnowflakeIdPacker();
                long jiveCurrentTime = new JiveEpochTimestampProvider().getTimestamp();
                final long packCurrentTime = snowflakeIdPacker.pack(jiveCurrentTime, 0, 0);
                final long fromTime = packCurrentTime - snowflakeIdPacker.pack(TimeUnit.HOURS.toMillis(fromHoursAgo), 0, 0);
                final long toTime = packCurrentTime - snowflakeIdPacker.pack(TimeUnit.HOURS.toMillis(toHoursAgo), 0, 0);

                MiruFilter constraintsFilter = filterStringUtil.parseFilters(input.filters);
                /*
                List<MiruFieldFilter> fieldFilters = Lists.newArrayList();
                fieldFilters.add(new MiruFieldFilter(MiruFieldType.primary, "locale", Collections.singletonList("en")));
                MiruFilter constraintsFilter = new MiruFilter(MiruFilterOperation.and, false, fieldFilters, null);
                */

                MiruResponse<DistinctsAnswer> response = null;
                if (!input.tenant.trim().isEmpty()) {
                    MiruTenantId tenantId = new MiruTenantId(input.tenant.trim().getBytes(Charsets.UTF_8));
                    String endpoint = DistinctsConstants.DISTINCTS_PREFIX + DistinctsConstants.CUSTOM_QUERY_ENDPOINT;
                    MiruRequest<DistinctsQuery> miruRequest = new MiruRequest<>("toolsDistincts",
                        tenantId,
                        MiruActorId.NOT_PROVIDED,
                        MiruAuthzExpression.NOT_PROVIDED,
                        new DistinctsQuery(
                            new MiruTimeRange(fromTime, toTime),
                            input.field,
                            null,
                            constraintsFilter,
                            fieldTypes),
                        MiruSolutionLogLevel.valueOf(input.logLevel));


                    MiruResponse<DistinctsAnswer> distinctsResponse = routing.query("", "distinctsPluginRegion",
                        miruRequest, endpoint, DistinctsAnswer.class);

                    if (distinctsResponse != null && distinctsResponse.answer != null) {
                        response = distinctsResponse;
                    } else {
                        log.warn("Empty distincts response from {}", tenantId);
                    }
                }

                if (response != null && response.answer != null) {
                    List<MiruValue> firstNValues = response.answer.results.subList(0, Math.min(input.maxCount, response.answer.results.size()));
                    data.put("elapse", String.valueOf(response.totalElapsed));
                    data.put("count", response.answer.results.size());
                    data.put("distincts", Lists.transform(firstNValues, MiruValue::last));

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
        return "Distincts";
    }
}
