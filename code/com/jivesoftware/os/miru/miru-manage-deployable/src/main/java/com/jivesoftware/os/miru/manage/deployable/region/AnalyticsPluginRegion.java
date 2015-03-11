package com.jivesoftware.os.miru.manage.deployable.region;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.google.common.base.Charsets;
import com.google.common.base.Joiner;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.jivesoftware.os.jive.utils.http.client.rest.RequestHelper;
import com.jivesoftware.os.jive.utils.ordered.id.JiveEpochTimestampProvider;
import com.jivesoftware.os.jive.utils.ordered.id.SnowflakeIdPacker;
import com.jivesoftware.os.miru.analytics.plugins.analytics.AnalyticsAnswer;
import com.jivesoftware.os.miru.analytics.plugins.analytics.AnalyticsConstants;
import com.jivesoftware.os.miru.analytics.plugins.analytics.AnalyticsQuery;
import com.jivesoftware.os.miru.api.MiruActorId;
import com.jivesoftware.os.miru.api.MiruHost;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.api.field.MiruFieldType;
import com.jivesoftware.os.miru.api.query.filter.FilterStringUtil;
import com.jivesoftware.os.miru.api.query.filter.MiruAuthzExpression;
import com.jivesoftware.os.miru.api.query.filter.MiruFieldFilter;
import com.jivesoftware.os.miru.api.query.filter.MiruFilter;
import com.jivesoftware.os.miru.api.query.filter.MiruFilterOperation;
import com.jivesoftware.os.miru.cluster.client.ReaderRequestHelpers;
import com.jivesoftware.os.miru.manage.deployable.MiruSoyRenderer;
import com.jivesoftware.os.miru.manage.deployable.analytics.MinMaxDouble;
import com.jivesoftware.os.miru.plugin.solution.MiruRequest;
import com.jivesoftware.os.miru.plugin.solution.MiruResponse;
import com.jivesoftware.os.miru.plugin.solution.MiruSolutionLogLevel;
import com.jivesoftware.os.miru.plugin.solution.MiruTimeRange;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

/**
 *
 */
// soy.miru.page.analyticsPluginRegion
public class AnalyticsPluginRegion implements MiruPageRegion<Optional<AnalyticsPluginRegion.AnalyticsPluginRegionInput>> {

    private static final MetricLogger log = MetricLoggerFactory.getLogger();

    private final String template;
    private final MiruSoyRenderer renderer;
    private final ReaderRequestHelpers readerRequestHelpers;
    private final FilterStringUtil filterStringUtil = new FilterStringUtil();

    public AnalyticsPluginRegion(String template,
        MiruSoyRenderer renderer,
        ReaderRequestHelpers readerRequestHelpers) {
        this.template = template;
        this.renderer = renderer;
        this.readerRequestHelpers = readerRequestHelpers;
    }

    public static class AnalyticsPluginRegionInput {

        final String tenant;
        final int fromHoursAgo;
        final int toHoursAgo;
        final int buckets;
        final String field1;
        final String terms1;
        final String field2;
        final String terms2;
        final String filters;
        final String logLevel;

        public AnalyticsPluginRegionInput(String tenant,
            int fromHoursAgo,
            int toHoursAgo,
            int buckets,
            String field1,
            String terms1,
            String field2,
            String terms2, String filters, String logLevel) {
            this.tenant = tenant;
            this.fromHoursAgo = fromHoursAgo;
            this.toHoursAgo = toHoursAgo;
            this.buckets = buckets;
            this.field1 = field1;
            this.terms1 = terms1;
            this.field2 = field2;
            this.terms2 = terms2;
            this.filters = filters;
            this.logLevel = logLevel;
        }
    }

    @Override
    public String render(Optional<AnalyticsPluginRegionInput> optionalInput) {
        Map<String, Object> data = Maps.newHashMap();
        try {
            if (optionalInput.isPresent()) {
                AnalyticsPluginRegionInput input = optionalInput.get();
                int fromHoursAgo = input.fromHoursAgo > input.toHoursAgo ? input.fromHoursAgo : input.toHoursAgo;
                int toHoursAgo = input.fromHoursAgo > input.toHoursAgo ? input.toHoursAgo : input.fromHoursAgo;

                data.put("logLevel", input.logLevel);
                data.put("tenant", input.tenant);
                data.put("fromHoursAgo", String.valueOf(fromHoursAgo));
                data.put("toHoursAgo", String.valueOf(toHoursAgo));
                data.put("buckets", String.valueOf(input.buckets));
                data.put("field1", input.field1);
                data.put("terms1", input.terms1);
                data.put("field2", input.field2);
                data.put("terms2", input.terms2);
                data.put("filters", input.filters);

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
                long jiveCurrentTime = new JiveEpochTimestampProvider().getTimestamp();
                final long packCurrentTime = snowflakeIdPacker.pack(jiveCurrentTime, 0, 0);
                final long fromTime = packCurrentTime - snowflakeIdPacker.pack(TimeUnit.HOURS.toMillis(fromHoursAgo), 0, 0);
                final long toTime = packCurrentTime - snowflakeIdPacker.pack(TimeUnit.HOURS.toMillis(toHoursAgo), 0, 0);

                MiruFilter constraintsFilter = filterStringUtil.parse(input.filters);

                List<RequestHelper> requestHelpers = readerRequestHelpers.get(Optional.<MiruHost>absent());
                MiruResponse<AnalyticsAnswer> response = null;
                if (!input.tenant.trim().isEmpty()) {
                    MiruTenantId tenantId = new MiruTenantId(input.tenant.trim().getBytes(Charsets.UTF_8));
                    for (RequestHelper requestHelper : requestHelpers) {
                        try {
                            ImmutableMap.Builder<String, MiruFilter> analyticsFiltersBuilder = ImmutableMap.builder();
                            for (String term1 : terms1) {
                                if (input.field2.isEmpty() || terms2.isEmpty()) {
                                    analyticsFiltersBuilder.put(
                                        termString(input.field1, term1),
                                        new MiruFilter(MiruFilterOperation.and,
                                            false,
                                            Collections.singletonList(
                                                new MiruFieldFilter(MiruFieldType.primary,
                                                    input.field1,
                                                    Collections.singletonList(term1))),
                                            null));
                                } else {
                                    for (String term2 : terms2) {
                                        analyticsFiltersBuilder.put(
                                            termString(input.field1, term1) + ", " + termString(input.field2, term2),
                                            new MiruFilter(MiruFilterOperation.and,
                                                false,
                                                Arrays.asList(
                                                    new MiruFieldFilter(MiruFieldType.primary,
                                                        input.field1,
                                                        Collections.singletonList(term1)),
                                                    new MiruFieldFilter(MiruFieldType.primary,
                                                        input.field2,
                                                        Collections.singletonList(term2))
                                                ),
                                                null));
                                    }
                                }
                            }
                            ImmutableMap<String, MiruFilter> analyticsFilters = analyticsFiltersBuilder.build();

                            @SuppressWarnings("unchecked")
                            MiruResponse<AnalyticsAnswer> analyticsResponse = requestHelper.executeRequest(
                                new MiruRequest<>(tenantId, MiruActorId.NOT_PROVIDED, MiruAuthzExpression.NOT_PROVIDED,
                                    new AnalyticsQuery(
                                        new MiruTimeRange(fromTime, toTime),
                                        input.buckets,
                                        constraintsFilter,
                                        analyticsFilters),
                                    MiruSolutionLogLevel.valueOf(input.logLevel)),
                                AnalyticsConstants.ANALYTICS_PREFIX + AnalyticsConstants.CUSTOM_QUERY_ENDPOINT, MiruResponse.class,
                                new Class[] { AnalyticsAnswer.class },
                                null);
                            response = analyticsResponse;
                            if (response != null && response.answer != null) {
                                break;
                            } else {
                                log.warn("Empty analytics response from {}, trying another", requestHelper);
                            }
                        } catch (Exception e) {
                            log.warn("Failed analytics request to {}, trying another", new Object[] { requestHelper }, e);
                        }
                    }
                }

                if (response != null && response.answer != null) {
                    Map<String, AnalyticsAnswer.Waveform> waveforms = response.answer.waveforms;
                    if (waveforms == null) {
                        waveforms = Collections.emptyMap();
                    }
                    data.put("elapse", String.valueOf(response.totalElapsed));
                    //data.put("waveform", waveform == null ? "" : waveform.toString());

                    data.put("waveform", "data:image/png;base64," + new PNGWaveforms().hitsToBase64PNGWaveform(1024, 400, 32, waveforms,
                        Optional.<MinMaxDouble>absent()));
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

    private String termString(String field, String term) {
        if (field.equals("activityType")) {
            return term + "=" + Type.valueOf(Integer.parseInt(term)).name();
        } else {
            return term;
        }
    }

    @Override
    public String getTitle() {
        return "Analytics";
    }

    public static enum Type {

        viewed(0),
        created(1),
        modified(2),
        commented(3),
        replied(4),
        voted(5),
        completed(6),
        updatedStatus(7),
        bookmarked(8),
        rated(9),
        blank(10),
        liked(11),
        joined(12),
        connected(13),
        followed(14),
        unfollowed(15),
        read(16),
        shared(17),
        NOTFOUND(18),
        UNAUTHORIZED(19),
        mentioned(20),
        promoted(21),
        clicked(22),
        logged_in(23),
        logged_out(24),
        applied(25),
        removed(26),
        repost(27),
        object_exclusion_added(28),
        object_exclusion_removed(29),
        context_exclusion_added(30),
        context_exclusion_removed(31),
        user_deleted(32),
        unread(33),
        register_database(34),
        manage(35),
        unmanage(36),
        tracked(37),
        untracked(38),
        allread(39),
        inUserStream(40),
        inUserInBox(41),
        inUserActivityQueue(42),
        unliked(43),
        projectCompleted(44),
        disinterest(45),
        notification(46),
        watch(47),
        unwatch(48),
        dismiss(49),
        unconnected(50),
        reshred_complete(51),
        unjoined(52),
        trace(53),
        heartbeat(54),
        moved(55),
        repairFollowHint(56),
        search(57),
        user_search(58),
        object_untrack_added(59),
        object_untrack_removed(60),
        digest(61),
        correct_answer_set(62),
        correct_answer_removed(63),
        tagged(64),
        outcome_set(65),
        outcome_removed(66),
        object_deleted(67),
        outcomes_modified(68),
        acclaim_added(69),
        acclaim_removed(70),
        acclaim_modified(71);

        private static Map<Integer, Type> typeMap = new ConcurrentHashMap<>();

        static {
            for (Type type : values()) {
                typeMap.put(type.getID(), type);
            }
        }

        private int id;

        Type(int id) {
            this.id = id;
        }

        public int getID() {
            return id;
        }

        public static Type valueOf(int id) {
            return typeMap.get(id);
        }
    }
}
