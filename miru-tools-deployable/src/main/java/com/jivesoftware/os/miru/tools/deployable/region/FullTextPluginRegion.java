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
import com.jivesoftware.os.miru.api.MiruHost;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.api.query.filter.FilterStringUtil;
import com.jivesoftware.os.miru.api.query.filter.MiruAuthzExpression;
import com.jivesoftware.os.miru.api.query.filter.MiruFilter;
import com.jivesoftware.os.miru.api.topology.ReaderRequestHelpers;
import com.jivesoftware.os.miru.plugin.solution.MiruRequest;
import com.jivesoftware.os.miru.plugin.solution.MiruResponse;
import com.jivesoftware.os.miru.plugin.solution.MiruSolutionLogLevel;
import com.jivesoftware.os.miru.plugin.solution.MiruTimeRange;
import com.jivesoftware.os.miru.stream.plugins.fulltext.FullTextAnswer;
import com.jivesoftware.os.miru.stream.plugins.fulltext.FullTextConstants;
import com.jivesoftware.os.miru.stream.plugins.fulltext.FullTextQuery;
import com.jivesoftware.os.miru.ui.MiruPageRegion;
import com.jivesoftware.os.miru.ui.MiruSoyRenderer;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import com.jivesoftware.os.routing.bird.http.client.HttpRequestHelper;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 *
 */
// soy.miru.page.fullTextPluginRegion
public class FullTextPluginRegion implements MiruPageRegion<Optional<FullTextPluginRegion.FullTextPluginRegionInput>> {

    private static final MetricLogger log = MetricLoggerFactory.getLogger();

    private final String template;
    private final MiruSoyRenderer renderer;
    private final ReaderRequestHelpers readerRequestHelpers;
    private final FilterStringUtil filterStringUtil = new FilterStringUtil();

    public FullTextPluginRegion(String template,
        MiruSoyRenderer renderer,
        ReaderRequestHelpers readerRequestHelpers) {
        this.template = template;
        this.renderer = renderer;
        this.readerRequestHelpers = readerRequestHelpers;
    }

    public static class FullTextPluginRegionInput {

        final String tenant;
        final int fromHoursAgo;
        final int toHoursAgo;
        final String defaultField;
        final String queryString;
        final FullTextQuery.Strategy strategy;
        final String filters;
        final int maxCount;
        final String logLevel;

        public FullTextPluginRegionInput(String tenant,
            int fromHoursAgo,
            int toHoursAgo,
            String defaultField,
            String queryString,
            FullTextQuery.Strategy strategy,
            String filters,
            int maxCount,
            String logLevel) {
            this.tenant = tenant;
            this.fromHoursAgo = fromHoursAgo;
            this.toHoursAgo = toHoursAgo;
            this.defaultField = defaultField;
            this.queryString = queryString;
            this.strategy = strategy;
            this.filters = filters;
            this.maxCount = maxCount;
            this.logLevel = logLevel;
        }
    }

    @Override
    public String render(Optional<FullTextPluginRegionInput> optionalInput) {
        Map<String, Object> data = Maps.newHashMap();
        try {
            if (optionalInput.isPresent()) {
                FullTextPluginRegionInput input = optionalInput.get();
                int fromHoursAgo = input.fromHoursAgo > input.toHoursAgo ? input.fromHoursAgo : input.toHoursAgo;
                int toHoursAgo = input.fromHoursAgo > input.toHoursAgo ? input.toHoursAgo : input.fromHoursAgo;

                data.put("logLevel", input.logLevel);
                data.put("tenant", input.tenant);
                data.put("fromHoursAgo", String.valueOf(fromHoursAgo));
                data.put("toHoursAgo", String.valueOf(toHoursAgo));
                data.put("defaultField", input.defaultField);
                data.put("queryString", input.queryString);
                data.put("strategy", input.strategy.name());
                data.put("maxCount", input.maxCount);
                data.put("filters", input.filters);

                SnowflakeIdPacker snowflakeIdPacker = new SnowflakeIdPacker();
                long jiveCurrentTime = new JiveEpochTimestampProvider().getTimestamp();
                final long packCurrentTime = snowflakeIdPacker.pack(jiveCurrentTime, 0, 0);
                final long fromTime = packCurrentTime - snowflakeIdPacker.pack(TimeUnit.HOURS.toMillis(fromHoursAgo), 0, 0);
                final long toTime = packCurrentTime - snowflakeIdPacker.pack(TimeUnit.HOURS.toMillis(toHoursAgo), 0, 0);

                MiruFilter constraintsFilter = filterStringUtil.parse(input.filters);

                List<HttpRequestHelper> requestHelpers = readerRequestHelpers.get(Optional.<MiruHost>absent());
                MiruResponse<FullTextAnswer> response = null;
                if (!input.tenant.trim().isEmpty()) {
                    MiruTenantId tenantId = new MiruTenantId(input.tenant.trim().getBytes(Charsets.UTF_8));
                    for (HttpRequestHelper requestHelper : requestHelpers) {
                        try {
                            @SuppressWarnings("unchecked")
                            MiruResponse<FullTextAnswer> fullTextResponse = requestHelper.executeRequest(
                                new MiruRequest<>("toolsFullText",
                                    tenantId,
                                    MiruActorId.NOT_PROVIDED,
                                    MiruAuthzExpression.NOT_PROVIDED,
                                    new FullTextQuery(
                                        new MiruTimeRange(fromTime, toTime),
                                        input.defaultField,
                                        input.queryString,
                                        constraintsFilter,
                                        input.strategy,
                                        input.maxCount),
                                    MiruSolutionLogLevel.valueOf(input.logLevel)),
                                FullTextConstants.FULLTEXT_PREFIX + FullTextConstants.CUSTOM_QUERY_ENDPOINT,
                                MiruResponse.class,
                                new Class[] { FullTextAnswer.class },
                                null);
                            response = fullTextResponse;
                            if (response != null && response.answer != null) {
                                break;
                            } else {
                                log.warn("Empty full text response from {}, trying another", requestHelper);
                            }
                        } catch (Exception e) {
                            log.warn("Failed full text request to {}, trying another", new Object[] { requestHelper }, e);
                        }
                    }
                }

                if (response != null && response.answer != null) {
                    data.put("elapse", String.valueOf(response.totalElapsed));
                    data.put("count", response.answer.results.size());
                    List<FullTextAnswer.ActivityScore> scores = response.answer.results.subList(0, Math.min(1_000, response.answer.results.size()));
                    List<Map<String, Object>> results = new ArrayList<>();
                    for (FullTextAnswer.ActivityScore score : scores) {
                        Map<String, Object> result = new HashMap<>();
                        result.put("activity", score.activity.toString());
                        result.put("score", String.valueOf(score.score));
                        results.add(result);
                    }
                    data.put("results", results);

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
        return "Full Text";
    }
}
