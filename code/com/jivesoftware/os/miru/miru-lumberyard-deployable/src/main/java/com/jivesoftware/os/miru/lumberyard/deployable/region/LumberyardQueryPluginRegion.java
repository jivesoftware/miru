package com.jivesoftware.os.miru.lumberyard.deployable.region;

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
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.api.field.MiruFieldType;
import com.jivesoftware.os.miru.api.query.filter.MiruAuthzExpression;
import com.jivesoftware.os.miru.api.query.filter.MiruFieldFilter;
import com.jivesoftware.os.miru.api.query.filter.MiruFilter;
import com.jivesoftware.os.miru.api.query.filter.MiruFilterOperation;
import com.jivesoftware.os.miru.lumberyard.deployable.MiruSoyRenderer;
import com.jivesoftware.os.miru.lumberyard.deployable.analytics.MinMaxDouble;
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
// soy.miru.page.lumberyardQueryPluginRegion
public class LumberyardQueryPluginRegion implements MiruPageRegion<Optional<LumberyardQueryPluginRegion.LumberyardPluginRegionInput>> {

    private static final MetricLogger log = MetricLoggerFactory.getLogger();

    private final String template;
    private final MiruSoyRenderer renderer;
    private final RequestHelper[] miruReaders;

    public LumberyardQueryPluginRegion(String template,
        MiruSoyRenderer renderer,
        RequestHelper[] miruReaders) {
        this.template = template;
        this.renderer = renderer;
        this.miruReaders = miruReaders;
    }

    public static class LumberyardPluginRegionInput {

        final String cluster;
        final String host;
        final String version;
        final String service;
        final String instance;

        final String logLevel;
        final int fromAgo;
        final int toAgo;
        final String timeUnit;

        final String thread;
        final String logger;
        final String message;

        //@Deprecated
        final String tenant;
        final int buckets;
        final int messageCount;
        //@Deprecated
        final String activityTypes;
        //@Deprecated
        final String users;

        public LumberyardPluginRegionInput(String cluster,
            String host,
            String version,
            String service,
            String instance,
            String logLevel,
            int fromAgo,
            int toAgo,
            String timeUnit,
            String thread,
            String logger,
            String message,
            String tenant,
            int buckets,
            int messageCount,
            String activityTypes,
            String users) {

            this.cluster = cluster;
            this.host = host;
            this.version = version;
            this.service = service;
            this.instance = instance;
            this.logLevel = logLevel;
            this.fromAgo = fromAgo;
            this.toAgo = toAgo;
            this.timeUnit = timeUnit;
            this.thread = thread;
            this.logger = logger;
            this.message = message;
            this.tenant = tenant;
            this.buckets = buckets;
            this.messageCount = messageCount;
            this.activityTypes = activityTypes;
            this.users = users;
        }

    }

    @Override
    public String render(Optional<LumberyardPluginRegionInput> optionalInput) {
        Map<String, Object> data = Maps.newHashMap();
        try {
            if (optionalInput.isPresent()) {
                LumberyardPluginRegionInput input = optionalInput.get();
                int fromAgo = input.fromAgo > input.toAgo ? input.fromAgo : input.toAgo;
                int toAgo = input.fromAgo > input.toAgo ? input.toAgo : input.fromAgo;

                data.put("cluster", input.cluster);
                data.put("host", input.host);
                data.put("version", input.version);
                data.put("service", input.service);
                data.put("instance", input.instance);
                data.put("timeUnit", input.timeUnit);
                data.put("thread", input.thread);
                data.put("logger", input.logger);
                data.put("message", input.message);

                data.put("logLevel", input.logLevel);
                data.put("tenant", input.tenant);
                data.put("fromAgo", String.valueOf(fromAgo));
                data.put("toAgo", String.valueOf(toAgo));
                data.put("buckets", String.valueOf(input.buckets));
                data.put("messageCount", String.valueOf(input.messageCount));

                data.put("activityTypes", input.activityTypes);
                data.put("users", input.users);

                List<String> activityTypes = Lists.newArrayList();
                for (String activityType : input.activityTypes.split(",")) {
                    String trimmed = activityType.trim();
                    if (!trimmed.isEmpty()) {
                        activityTypes.add(trimmed);
                    }
                }

                List<String> users = Lists.newArrayList();
                for (String user : input.users.split(",")) {
                    String trimmed = user.trim();
                    if (!trimmed.isEmpty()) {
                        users.add(trimmed);
                    }
                }

                SnowflakeIdPacker snowflakeIdPacker = new SnowflakeIdPacker();
                long jiveCurrentTime = new JiveEpochTimestampProvider().getTimestamp();
                final long packCurrentTime = snowflakeIdPacker.pack(jiveCurrentTime, 0, 0);
                final long fromTime = packCurrentTime - snowflakeIdPacker.pack(TimeUnit.valueOf(input.timeUnit).toMillis(fromAgo), 0, 0);
                final long toTime = packCurrentTime - snowflakeIdPacker.pack(TimeUnit.valueOf(input.timeUnit).toMillis(toAgo), 0, 0);
                List<MiruFieldFilter> fieldFilters = Lists.newArrayList();
                fieldFilters.add(new MiruFieldFilter(MiruFieldType.primary, "locale", Collections.singletonList("en")));

                MiruFilter constraintsFilter = new MiruFilter(MiruFilterOperation.and,
                    false,
                    fieldFilters,
                    null);

                MiruResponse<AnalyticsAnswer> response = null;
                if (!input.tenant.trim().isEmpty()) {
                    MiruTenantId tenantId = new MiruTenantId(input.tenant.trim().getBytes(Charsets.UTF_8));
                    for (RequestHelper requestHelper : miruReaders) {
                        try {
                            ImmutableMap.Builder<String, MiruFilter> analyticsFiltersBuilder = ImmutableMap.builder();
                            for (String activityType : activityTypes) {
                                if (users.isEmpty()) {
                                    analyticsFiltersBuilder.put(
                                        activityType + "=" + Type.valueOf(Integer.parseInt(activityType)).name(),
                                        new MiruFilter(MiruFilterOperation.and,
                                            false,
                                            Collections.singletonList(
                                                new MiruFieldFilter(MiruFieldType.primary,
                                                    "activityType",
                                                    Collections.singletonList(activityType))),
                                            null));
                                } else {
                                    for (String user : users) {
                                        analyticsFiltersBuilder.put(
                                            activityType + "=" + Type.valueOf(Integer.parseInt(activityType)).name() + ", user=" + user,
                                            new MiruFilter(MiruFilterOperation.and,
                                                false,
                                                Arrays.asList(
                                                    new MiruFieldFilter(MiruFieldType.primary,
                                                        "activityType",
                                                        Collections.singletonList(activityType)),
                                                    new MiruFieldFilter(MiruFieldType.primary,
                                                        "user",
                                                        Collections.singletonList("3 " + user))
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
                                new Class[]{AnalyticsAnswer.class},
                                null);
                            response = analyticsResponse;
                            if (response != null && response.answer != null) {
                                break;
                            } else {
                                log.warn("Empty analytics response from {}, trying another", requestHelper);
                            }
                        } catch (Exception e) {
                            log.warn("Failed analytics request to {}, trying another", new Object[]{requestHelper}, e);
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
                    data.put("evetns", Arrays.asList(new String[]{
                        "Event1",
                        "Event2"
                    }));
                }
            }
        } catch (Exception e) {
            log.error("Unable to retrieve data", e);
        }

        return renderer.render(template, data);
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
