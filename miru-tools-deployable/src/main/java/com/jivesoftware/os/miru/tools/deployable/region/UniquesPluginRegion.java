package com.jivesoftware.os.miru.tools.deployable.region;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.google.common.base.Charsets;
import com.google.common.base.Joiner;
import com.google.common.base.Optional;
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
import com.jivesoftware.os.miru.reco.plugins.uniques.UniquesAnswer;
import com.jivesoftware.os.miru.reco.plugins.uniques.UniquesConstants;
import com.jivesoftware.os.miru.reco.plugins.uniques.UniquesQuery;
import com.jivesoftware.os.miru.ui.MiruPageRegion;
import com.jivesoftware.os.miru.ui.MiruSoyRenderer;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

// soy.miru.page.uniquesPluginRegion
public class UniquesPluginRegion implements MiruPageRegion<Optional<UniquesPluginRegion.UniquesPluginRegionInput>> {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    private final String template;
    private final MiruSoyRenderer renderer;
    private final MiruRouting routing;
    private final FilterStringUtil filterStringUtil;

    public UniquesPluginRegion(String template,
        MiruSoyRenderer renderer,
        MiruRouting routing,
        FilterStringUtil filterStringUtil) {
        this.template = template;
        this.renderer = renderer;
        this.routing = routing;
        this.filterStringUtil = filterStringUtil;
    }

    public static class UniquesPluginRegionInput {

        final String tenant;
        final int fromHoursAgo;
        final int toHoursAgo;
        final String field;
        final String types;
        final String filters;
        final String logLevel;

        public UniquesPluginRegionInput(String tenant,
            int fromHoursAgo,
            int toHoursAgo,
            String field,
            String types,
            String filters,
            String logLevel) {
            this.tenant = tenant;
            this.fromHoursAgo = fromHoursAgo;
            this.toHoursAgo = toHoursAgo;
            this.field = field;
            this.types = types;
            this.filters = filters;
            this.logLevel = logLevel;
        }
    }

    @Override
    public String render(Optional<UniquesPluginRegionInput> optionalInput) {
        Map<String, Object> data = Maps.newHashMap();
        try {
            if (optionalInput.isPresent()) {
                UniquesPluginRegionInput input = optionalInput.get();
                int fromHoursAgo = input.fromHoursAgo > input.toHoursAgo ? input.fromHoursAgo : input.toHoursAgo;
                int toHoursAgo = input.fromHoursAgo > input.toHoursAgo ? input.toHoursAgo : input.fromHoursAgo;

                data.put("logLevel", input.logLevel);
                data.put("tenant", input.tenant);
                data.put("fromHoursAgo", String.valueOf(fromHoursAgo));
                data.put("toHoursAgo", String.valueOf(toHoursAgo));
                data.put("field", input.field);
                data.put("types", input.types);
                data.put("filters", input.filters);

                List<MiruValue> fieldTypes = filterStringUtil.buildFieldPrefixes(Arrays.asList(input.types.split(",")));

                SnowflakeIdPacker snowflakeIdPacker = new SnowflakeIdPacker();
                long jiveCurrentTime = new JiveEpochTimestampProvider().getTimestamp();
                final long packCurrentTime = snowflakeIdPacker.pack(jiveCurrentTime, 0, 0);
                final long fromTime = packCurrentTime - snowflakeIdPacker.pack(TimeUnit.HOURS.toMillis(fromHoursAgo), 0, 0);
                final long toTime = packCurrentTime - snowflakeIdPacker.pack(TimeUnit.HOURS.toMillis(toHoursAgo), 0, 0);

                MiruFilter constraintsFilter = filterStringUtil.parseFilters(input.filters);

                MiruResponse<UniquesAnswer> response = null;
                if (!input.tenant.trim().isEmpty()) {
                    MiruTenantId tenantId = new MiruTenantId(input.tenant.trim().getBytes(Charsets.UTF_8));

                    String endpoint = UniquesConstants.UNIQUES_PREFIX + UniquesConstants.CUSTOM_QUERY_ENDPOINT;

                    MiruRequest<UniquesQuery> miruRequest = new MiruRequest<>(
                        "toolsUniques",
                        tenantId,
                        MiruActorId.NOT_PROVIDED,
                        MiruAuthzExpression.NOT_PROVIDED,
                        new UniquesQuery(
                            new MiruTimeRange(fromTime, toTime),
                            input.field,
                            null,
                            constraintsFilter,
                            fieldTypes),
                        MiruSolutionLogLevel.valueOf(input.logLevel));

                    MiruResponse<UniquesAnswer> uniquesResponse = routing.query("", "uniquesPluginRegion",
                        miruRequest, endpoint, UniquesAnswer.class);

                    if (uniquesResponse != null && uniquesResponse.answer != null) {
                        response = uniquesResponse;
                        LOG.warn("Uniques answer {} from {}",
                            uniquesResponse.answer.uniques, tenantId);
                    } else {
                        LOG.warn("Empty uniques response from {}", tenantId);
                    }
                }

                if (response != null && response.answer != null) {
                    data.put("uniques", String.valueOf(response.answer.uniques));

                    ObjectMapper mapper = new ObjectMapper();
                    mapper.enable(SerializationFeature.INDENT_OUTPUT);
                    data.put("summary", Joiner.on("\n").join(response.log) + "\n\n" + mapper.writeValueAsString(response.solutions));
                    data.put("elapse", String.valueOf(response.totalElapsed));
                }
            }
        } catch (Exception e) {
            LOG.error("Unable to retrieve data", e);
        }

        return renderer.render(template, data);
    }

    @Override
    public String getTitle() {
        return "Uniques";
    }

}
