package com.jivesoftware.os.miru.manage.deployable.region;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.google.common.base.Charsets;
import com.google.common.base.Function;
import com.google.common.base.Functions;
import com.google.common.base.Joiner;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.jivesoftware.os.jive.utils.http.client.rest.RequestHelper;
import com.jivesoftware.os.jive.utils.logger.MetricLogger;
import com.jivesoftware.os.jive.utils.logger.MetricLoggerFactory;
import com.jivesoftware.os.jive.utils.ordered.id.JiveEpochTimestampProvider;
import com.jivesoftware.os.jive.utils.ordered.id.SnowflakeIdPacker;
import com.jivesoftware.os.miru.api.MiruActorId;
import com.jivesoftware.os.miru.api.MiruHost;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.api.field.MiruFieldType;
import com.jivesoftware.os.miru.api.query.filter.MiruAuthzExpression;
import com.jivesoftware.os.miru.api.query.filter.MiruFieldFilter;
import com.jivesoftware.os.miru.api.query.filter.MiruFilter;
import com.jivesoftware.os.miru.api.query.filter.MiruFilterOperation;
import com.jivesoftware.os.miru.manage.deployable.MiruSoyRenderer;
import com.jivesoftware.os.miru.manage.deployable.ReaderRequestHelpers;
import com.jivesoftware.os.miru.plugin.solution.MiruRequest;
import com.jivesoftware.os.miru.plugin.solution.MiruResponse;
import com.jivesoftware.os.miru.plugin.solution.MiruTimeRange;
import com.jivesoftware.os.miru.reco.plugins.trending.TrendingAnswer;
import com.jivesoftware.os.miru.reco.plugins.trending.TrendingConstants;
import com.jivesoftware.os.miru.reco.plugins.trending.TrendingQuery;
import com.jivesoftware.os.miru.reco.plugins.trending.Trendy;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 *
 */
// soy.miru.page.trendingPluginRegion
public class TrendingPluginRegion implements MiruPageRegion<Optional<TrendingPluginRegion.TrendingPluginRegionInput>> {

    private static final MetricLogger log = MetricLoggerFactory.getLogger();

    private final String template;
    private final MiruSoyRenderer renderer;
    private final ReaderRequestHelpers readerRequestHelpers;

    public TrendingPluginRegion(String template,
        MiruSoyRenderer renderer,
        ReaderRequestHelpers readerRequestHelpers) {
        this.template = template;
        this.renderer = renderer;
        this.readerRequestHelpers = readerRequestHelpers;
    }

    public static class TrendingPluginRegionInput {

        final String tenant;
        final int fromHoursAgo;
        final int toHoursAgo;
        final int buckets;
        final String field;

        public TrendingPluginRegionInput(String tenant, int fromHoursAgo, int toHoursAgo, int buckets, String field) {
            this.tenant = tenant;
            this.fromHoursAgo = fromHoursAgo;
            this.toHoursAgo = toHoursAgo;
            this.buckets = buckets;
            this.field = field;
        }
    }

    @Override
    public String render(Optional<TrendingPluginRegionInput> optionalInput) {
        Map<String, Object> data = Maps.newHashMap();
        try {
            if (optionalInput.isPresent()) {
                TrendingPluginRegionInput input = optionalInput.get();
                int fromHoursAgo = input.fromHoursAgo > input.toHoursAgo ? input.fromHoursAgo : input.toHoursAgo;
                int toHoursAgo = input.fromHoursAgo > input.toHoursAgo ? input.toHoursAgo : input.fromHoursAgo;

                data.put("tenant", input.tenant);
                data.put("fromHoursAgo", String.valueOf(fromHoursAgo));
                data.put("toHoursAgo", String.valueOf(toHoursAgo));
                data.put("buckets", String.valueOf(input.buckets));
                data.put("field", input.field);

                SnowflakeIdPacker snowflakeIdPacker = new SnowflakeIdPacker();
                long jiveCurrentTime = new JiveEpochTimestampProvider().getTimestamp();
                final long packCurrentTime = snowflakeIdPacker.pack(jiveCurrentTime, 0, 0);
                final long fromTime = packCurrentTime - snowflakeIdPacker.pack(TimeUnit.HOURS.toMillis(fromHoursAgo), 0, 0);
                final long toTime = packCurrentTime - snowflakeIdPacker.pack(TimeUnit.HOURS.toMillis(toHoursAgo), 0, 0);
                List<MiruFieldFilter> fieldFilters = Lists.newArrayList();
                fieldFilters.add(new MiruFieldFilter(MiruFieldType.primary, "activityType",
                    Lists.transform(
                        Arrays.asList(0, 1, 11, 65),
                        Functions.toStringFunction())));

                MiruFilter constraintsFilter = new MiruFilter(MiruFilterOperation.and,
                    Optional.of(fieldFilters),
                    Optional.<List<MiruFilter>>absent());

                List<RequestHelper> requestHelpers = readerRequestHelpers.get(Optional.<MiruHost>absent());
                MiruResponse<TrendingAnswer> response = null;
                if (!input.tenant.trim().isEmpty()) {
                    MiruTenantId tenantId = new MiruTenantId(input.tenant.trim().getBytes(Charsets.UTF_8));
                    for (RequestHelper requestHelper : requestHelpers) {
                        try {
                            @SuppressWarnings("unchecked")
                            MiruResponse<TrendingAnswer> trendingResponse = requestHelper.executeRequest(
                                new MiruRequest<>(tenantId, MiruActorId.NOT_PROVIDED, MiruAuthzExpression.NOT_PROVIDED,
                                    new TrendingQuery(
                                        new MiruTimeRange(fromTime, toTime),
                                        input.buckets,
                                        constraintsFilter,
                                        input.field,
                                        100),
                                    true),
                                TrendingConstants.TRENDING_PREFIX + TrendingConstants.CUSTOM_QUERY_ENDPOINT, MiruResponse.class,
                                new Class[] { TrendingAnswer.class },
                                null);
                            response = trendingResponse;
                            if (response != null && response.answer != null) {
                                break;
                            } else {
                                log.warn("Empty trending response from {}, trying another", requestHelper);
                            }
                        } catch (Exception e) {
                            log.warn("Failed trending request to {}, trying another", new Object[] { requestHelper }, e);
                        }
                    }
                }

                if (response != null && response.answer != null) {
                    List<Trendy> results = response.answer.results;
                    if (results == null) {
                        results = Collections.emptyList();
                    }
                    data.put("elapse", String.valueOf(response.totalElapsed));
                    //data.put("waveform", waveform == null ? "" : waveform.toString());

                    data.put("results", Lists.transform(results, new Function<Trendy, Map<String, String>>() {
                        @Override
                        public Map<String, String> apply(Trendy input) {
                            return ImmutableMap.of(
                                "name", new String(input.distinctValue, Charsets.UTF_8),
                                "rank", String.valueOf(input.rank));
                        }
                    }));
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
        return "Trending";
    }
}
