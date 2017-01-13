package com.jivesoftware.os.miru.tools.deployable.region;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.google.common.base.Charsets;
import com.google.common.base.Joiner;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.jivesoftware.os.jive.utils.ordered.id.JiveEpochTimestampProvider;
import com.jivesoftware.os.jive.utils.ordered.id.SnowflakeIdPacker;
import com.jivesoftware.os.miru.api.MiruActorId;
import com.jivesoftware.os.miru.api.base.MiruStreamId;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.api.query.filter.FilterStringUtil;
import com.jivesoftware.os.miru.api.query.filter.MiruAuthzExpression;
import com.jivesoftware.os.miru.api.query.filter.MiruFilter;
import com.jivesoftware.os.miru.plugin.solution.MiruRequest;
import com.jivesoftware.os.miru.plugin.solution.MiruResponse;
import com.jivesoftware.os.miru.plugin.solution.MiruSolutionLogLevel;
import com.jivesoftware.os.miru.plugin.solution.MiruTimeRange;
import com.jivesoftware.os.miru.stream.plugins.filter.AggregateCount;
import com.jivesoftware.os.miru.stream.plugins.filter.AggregateCountsAnswer;
import com.jivesoftware.os.miru.stream.plugins.filter.AggregateCountsConstants;
import com.jivesoftware.os.miru.stream.plugins.filter.AggregateCountsQuery;
import com.jivesoftware.os.miru.stream.plugins.filter.AggregateCountsQueryConstraint;
import com.jivesoftware.os.miru.ui.MiruPageRegion;
import com.jivesoftware.os.miru.ui.MiruSoyRenderer;
import com.jivesoftware.os.mlogger.core.ISO8601DateFormat;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import com.jivesoftware.os.routing.bird.http.client.HttpResponse;
import com.jivesoftware.os.routing.bird.http.client.HttpResponseMapper;
import com.jivesoftware.os.routing.bird.http.client.RoundRobinStrategy;
import com.jivesoftware.os.routing.bird.http.client.TenantAwareHttpClient;
import com.jivesoftware.os.routing.bird.shared.ClientCall.ClientResponse;
import java.util.Date;
import java.util.List;
import java.util.Map;

/**
 *
 */
// soy.miru.page.aggregateCountsPluginRegion
public class AggregateCountsPluginRegion implements MiruPageRegion<Optional<AggregateCountsPluginRegion.AggregateCountsPluginRegionInput>> {

    private static final MetricLogger log = MetricLoggerFactory.getLogger();

    private final String template;
    private final MiruSoyRenderer renderer;
    private final TenantAwareHttpClient<String> readerClient;
    private final ObjectMapper requestMapper;
    private final HttpResponseMapper responseMapper;
    private final FilterStringUtil filterStringUtil = new FilterStringUtil();

    public AggregateCountsPluginRegion(String template,
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

    public static class AggregateCountsPluginRegionInput {

        final String tenant;
        final String forUser;
        final boolean inbox;
        final long fromTimestamp;
        final String field;
        final String streamFilters;
        final String constraintsFilters;
        final int count;
        final int pages;
        final String logLevel;

        public AggregateCountsPluginRegionInput(String tenant,
            String forUser,
            boolean inbox,
            long fromTimestamp,
            String field,
            String streamFilters,
            String constraintsFilters,
            int count,
            int pages,
            String logLevel) {
            this.tenant = tenant;
            this.forUser = forUser;
            this.inbox = inbox;
            this.fromTimestamp = fromTimestamp;
            this.field = field;
            this.streamFilters = streamFilters;
            this.constraintsFilters = constraintsFilters;
            this.count = count;
            this.pages = pages;
            this.logLevel = logLevel;
        }
    }

    @Override
    public String render(Optional<AggregateCountsPluginRegionInput> optionalInput) {
        Map<String, Object> data = Maps.newHashMap();
        try {
            if (optionalInput.isPresent()) {
                AggregateCountsPluginRegionInput input = optionalInput.get();

                SnowflakeIdPacker snowflakeIdPacker = new SnowflakeIdPacker();

                data.put("logLevel", input.logLevel);
                data.put("tenant", input.tenant);
                data.put("forUser", input.forUser);
                data.put("inbox", input.inbox);
                data.put("fromTimestamp", String.valueOf(input.fromTimestamp));
                data.put("field", input.field);
                data.put("streamFilters", input.streamFilters);
                data.put("constraintsFilters", input.constraintsFilters);
                data.put("count", input.count);
                data.put("pages", input.pages);

                MiruFilter streamFilter = filterStringUtil.parseFilters(input.streamFilters);
                MiruFilter constraintsFilter = filterStringUtil.parseFilters(input.constraintsFilters);

                List<MiruResponse<AggregateCountsAnswer>> responses = Lists.newArrayList();
                if (!input.tenant.trim().isEmpty()) {
                    MiruTenantId tenantId = new MiruTenantId(input.tenant.trim().getBytes(Charsets.UTF_8));

                    MiruStreamId streamId;
                    if (input.forUser == null || input.forUser.isEmpty()) {
                        streamId = MiruStreamId.NULL;
                    } else {
                        streamId = new MiruStreamId(input.forUser.getBytes());
                    }

                    String endpoint;
                    if (input.inbox) {
                        endpoint = AggregateCountsConstants.FILTER_PREFIX + AggregateCountsConstants.INBOX_ALL_QUERY_ENDPOINT;
                    } else {
                        endpoint = AggregateCountsConstants.FILTER_PREFIX + AggregateCountsConstants.CUSTOM_QUERY_ENDPOINT;
                    }

                    MiruTimeRange timeRange = input.fromTimestamp > 0 ? new MiruTimeRange(0, input.fromTimestamp) : MiruTimeRange.ALL_TIME;

                    for (int i = 0; i < input.pages; i++) {
                        if (timeRange == null) {
                            break;
                        }
                        String request = requestMapper.writeValueAsString(new MiruRequest<>("toolsAggregateCounts",
                            tenantId,
                            MiruActorId.NOT_PROVIDED,
                            MiruAuthzExpression.NOT_PROVIDED,
                            new AggregateCountsQuery(
                                streamId,
                                null, //TODO this prevents backfill
                                MiruTimeRange.ALL_TIME,
                                timeRange,
                                MiruTimeRange.ALL_TIME,
                                streamFilter,
                                ImmutableMap.of(input.field,
                                    new AggregateCountsQueryConstraint(constraintsFilter,
                                        input.field,
                                        0,
                                        input.count,
                                        new String[0])),
                                false),
                            MiruSolutionLogLevel.valueOf(input.logLevel)));

                        MiruResponse<AggregateCountsAnswer> aggregatesResponse = readerClient.call("",
                            new RoundRobinStrategy(),
                            "aggregateCountsPluginRegion",
                            httpClient -> {
                                HttpResponse httpResponse = httpClient.postJson(endpoint, request, null);
                                @SuppressWarnings("unchecked")
                                MiruResponse<AggregateCountsAnswer> response = responseMapper.extractResultFromResponse(httpResponse,
                                    MiruResponse.class,
                                    new Class<?>[] { AggregateCountsAnswer.class },
                                    null);
                                return new ClientResponse<>(response, true);
                            });
                        if (aggregatesResponse != null && aggregatesResponse.answer != null) {
                            List<AggregateCount> results = aggregatesResponse.answer.constraints.get(input.field).results;
                            if (results.size() < input.count) {
                                timeRange = null;
                            } else {
                                long lastTimestamp = results.get(results.size() - 1).timestamp;
                                timeRange = new MiruTimeRange(0, lastTimestamp - 1);
                            }
                            responses.add(aggregatesResponse);
                            break;
                        } else {
                            log.warn("Empty aggregate counts response for {}", tenantId);
                        }
                    }
                }

                if (!responses.isEmpty()) {
                    ObjectMapper mapper = new ObjectMapper();
                    mapper.enable(SerializationFeature.INDENT_OUTPUT);

                    List<List<Map<String, Object>>> resultPages = Lists.newArrayList();
                    List<Map<String, Object>> summaries = Lists.newArrayList();
                    for (MiruResponse<AggregateCountsAnswer> response : responses) {
                        List<Map<String, Object>> page = Lists.newArrayList();
                        for (AggregateCount result : response.answer.constraints.get(input.field).results) {
                            long time = result.timestamp;
                            long jiveEpochTime = snowflakeIdPacker.unpack(time)[0];
                            String clockTime = new ISO8601DateFormat().format(new Date(jiveEpochTime + JiveEpochTimestampProvider.JIVE_EPOCH));

                            page.add(ImmutableMap.<String, Object>of(
                                "aggregate", result.distinctValue.last(),
                                "time", String.valueOf(time),
                                "date", clockTime,
                                "count", String.valueOf(result.count),
                                "unread", String.valueOf(result.unread)));
                        }
                        resultPages.add(page);

                        summaries.add(ImmutableMap.<String, Object>of(
                            "body", Joiner.on("\n").join(response.log) + "\n\n" + mapper.writeValueAsString(response.solutions),
                            "elapse", String.valueOf(response.totalElapsed)));
                    }

                    data.put("resultPages", resultPages);
                    data.put("summaries", summaries);
                }
            }
        } catch (
            Exception e
            )

        {
            log.error("Unable to retrieve data", e);
        }

        return renderer.render(template, data);
    }

    @Override
    public String getTitle() {
        return "Aggregate Counts";
    }
}
