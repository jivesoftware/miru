package com.jivesoftware.os.miru.stumptown.deployable.region;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.jivesoftware.os.jive.utils.http.client.rest.RequestHelper;
import com.jivesoftware.os.jive.utils.ordered.id.JiveEpochTimestampProvider;
import com.jivesoftware.os.jive.utils.ordered.id.SnowflakeIdPacker;
import com.jivesoftware.os.miru.api.MiruActorId;
import com.jivesoftware.os.miru.api.activity.MiruActivity;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.api.field.MiruFieldType;
import com.jivesoftware.os.miru.api.query.filter.MiruAuthzExpression;
import com.jivesoftware.os.miru.api.query.filter.MiruFieldFilter;
import com.jivesoftware.os.miru.api.query.filter.MiruFilter;
import com.jivesoftware.os.miru.api.query.filter.MiruFilterOperation;
import com.jivesoftware.os.miru.logappender.MiruLogEvent;
import com.jivesoftware.os.miru.plugin.solution.MiruRequest;
import com.jivesoftware.os.miru.plugin.solution.MiruResponse;
import com.jivesoftware.os.miru.plugin.solution.MiruSolutionLogLevel;
import com.jivesoftware.os.miru.plugin.solution.MiruTimeRange;
import com.jivesoftware.os.miru.stumptown.deployable.MiruSoyRenderer;
import com.jivesoftware.os.miru.stumptown.deployable.StumptownSchemaConstants;
import com.jivesoftware.os.miru.stumptown.deployable.storage.MiruStumptownPayloads;
import com.jivesoftware.os.miru.stumptown.plugins.StumptownAnswer;
import com.jivesoftware.os.miru.stumptown.plugins.StumptownConstants;
import com.jivesoftware.os.miru.stumptown.plugins.StumptownQuery;
import com.jivesoftware.os.mlogger.core.ISO8601DateFormat;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;

import static com.google.common.base.Objects.firstNonNull;

/**
 *
 */
// soy.stumptown.page.stumptownQueryPluginRegion
public class StumptownQueryPluginRegion implements PageRegion<Optional<StumptownQueryPluginRegion.StumptownPluginRegionInput>> {

    private static final MetricLogger log = MetricLoggerFactory.getLogger();

    private final String template;
    private final String logEventTemplate;
    private final String noEventsTemplate;
    private final MiruSoyRenderer renderer;
    private final RequestHelper[] miruReaders;
    private final MiruStumptownPayloads payloads;

    public StumptownQueryPluginRegion(String template,
        String logEventTemplate,
        String noEventsTemplate,
        MiruSoyRenderer renderer,
        RequestHelper[] miruReaders,
        MiruStumptownPayloads payloads) {

        this.template = template;
        this.logEventTemplate = logEventTemplate;
        this.noEventsTemplate = noEventsTemplate;
        this.renderer = renderer;
        this.miruReaders = miruReaders;
        this.payloads = payloads;
    }

    public static class StumptownPluginRegionInput {

        final String cluster;
        final String host;
        final String service;
        final String instance;
        final String version;

        final String logLevel;
        final int fromAgo;
        final int toAgo;
        final String fromTimeUnit;
        final String toTimeUnit;

        final String thread;
        final String logger;
        final String method;
        final String line;
        final String message;
        final String exceptionClass;
        final String thrown;

        final int buckets;
        final int messageCount;
        final String graphType;

        public StumptownPluginRegionInput(String cluster,
            String host,
            String service,
            String instance,
            String version,
            String logLevel,
            int fromAgo,
            int toAgo,
            String fromTimeUnit,
            String toTimeUnit,
            String thread,
            String logger,
            String method,
            String line,
            String message,
            String exceptionClass,
            String thrown,
            int buckets,
            int messageCount,
            String graphType) {

            this.cluster = cluster;
            this.host = host;
            this.service = service;
            this.instance = instance;
            this.version = version;
            this.logLevel = logLevel;
            this.fromAgo = fromAgo;
            this.toAgo = toAgo;
            this.fromTimeUnit = fromTimeUnit;
            this.toTimeUnit = toTimeUnit;
            this.thread = thread;
            this.logger = logger;
            this.method = method;
            this.line = line;
            this.message = message;
            this.exceptionClass = exceptionClass;
            this.thrown = thrown;
            this.buckets = buckets;
            this.messageCount = messageCount;
            this.graphType = graphType;
        }

    }

    @Override
    public String render(Optional<StumptownPluginRegionInput> optionalInput) {
        Map<String, Object> data = Maps.newHashMap();
        try {
            if (optionalInput.isPresent()) {
                StumptownPluginRegionInput input = optionalInput.get();
                int fromAgo = input.fromAgo > input.toAgo ? input.fromAgo : input.toAgo;
                int toAgo = input.fromAgo > input.toAgo ? input.toAgo : input.fromAgo;

                data.put("cluster", input.cluster);
                data.put("host", input.host);
                data.put("service", input.service);
                data.put("instance", input.instance);
                data.put("version", input.version);
                data.put("fromTimeUnit", input.fromTimeUnit);
                data.put("toTimeUnit", input.toTimeUnit);
                data.put("thread", input.thread);
                data.put("logger", input.logger);
                data.put("method", input.method);
                data.put("line", input.line);
                data.put("message", input.message);
                data.put("exceptionClass", input.exceptionClass);
                data.put("thrown", input.thrown);

                Set<String> logLevelSet = Sets.newHashSet(Splitter.on(',').split(input.logLevel));
                data.put("logLevels", ImmutableMap.of(
                    "trace", logLevelSet.contains("TRACE"),
                    "debug", logLevelSet.contains("DEBUG"),
                    "info", logLevelSet.contains("INFO"),
                    "warn", logLevelSet.contains("WARN"),
                    "error", logLevelSet.contains("ERROR")));
                data.put("logLevelsList", input.logLevel);
                data.put("fromAgo", String.valueOf(fromAgo));
                data.put("toAgo", String.valueOf(toAgo));
                data.put("buckets", String.valueOf(input.buckets));
                data.put("messageCount", String.valueOf(input.messageCount));
                data.put("graphType", input.graphType);

                boolean execute = !logLevelSet.isEmpty();
                data.put("execute", execute);
            }
        } catch (Exception e) {
            log.error("Unable to retrieve data", e);
        }
        return renderer.render(template, data);
    }

    public Map<String, Object> poll(StumptownPluginRegionInput input) throws Exception {
        Map<String, Object> data = Maps.newHashMap();

        int fromAgo = input.fromAgo > input.toAgo ? input.fromAgo : input.toAgo;
        int toAgo = input.fromAgo > input.toAgo ? input.toAgo : input.fromAgo;

        SnowflakeIdPacker snowflakeIdPacker = new SnowflakeIdPacker();
        // e.g. mpb=10,000, current=24,478, modulus=4,478, ceiling=30,000
        TimeUnit fromTimeUnit = TimeUnit.valueOf(input.fromTimeUnit);
        TimeUnit toTimeUnit = TimeUnit.valueOf(input.toTimeUnit);
        long rangeMillis = fromTimeUnit.toMillis(fromAgo) - toTimeUnit.toMillis(toAgo);
        long millisPerBucket = rangeMillis / input.buckets;
        long jiveToTime = new JiveEpochTimestampProvider().getTimestamp() - fromTimeUnit.toMillis(toAgo);
        long jiveModulusTime = jiveToTime % millisPerBucket;
        long jiveCeilingTime = jiveToTime - jiveModulusTime + millisPerBucket;
        final long packCeilingTime = snowflakeIdPacker.pack(jiveCeilingTime, 0, 0);
        final long packLookbackTime = packCeilingTime - snowflakeIdPacker.pack(rangeMillis, 0, 0);

        MiruTenantId tenantId = StumptownSchemaConstants.TENANT_ID;
        MiruResponse<StumptownAnswer> response = null;
        for (RequestHelper requestHelper : miruReaders) {
            try {
                List<MiruFieldFilter> fieldFilters = Lists.newArrayList();
                List<MiruFieldFilter> notFieldFilters = Lists.newArrayList();
                addFieldFilter(fieldFilters, notFieldFilters, "cluster", input.cluster);
                addFieldFilter(fieldFilters, notFieldFilters, "host", input.host);
                addFieldFilter(fieldFilters, notFieldFilters, "service", input.service);
                addFieldFilter(fieldFilters, notFieldFilters, "instance", input.instance);
                addFieldFilter(fieldFilters, notFieldFilters, "version", input.version);
                addFieldFilter(fieldFilters, notFieldFilters, "thread", input.thread);
                addFieldFilter(fieldFilters, notFieldFilters, "methodName", input.method);
                addFieldFilter(fieldFilters, notFieldFilters, "lineNumber", input.line);
                addFieldFilter(fieldFilters, notFieldFilters, "logger", input.logger);
                addFieldFilter(fieldFilters, notFieldFilters, "message", input.message.toLowerCase());
                addFieldFilter(fieldFilters, notFieldFilters, "level", input.logLevel);
                addFieldFilter(fieldFilters, notFieldFilters, "exceptionClass", input.exceptionClass);
                addFieldFilter(fieldFilters, notFieldFilters, "thrownStackTrace", input.thrown.toLowerCase());

                List<MiruFilter> notFilters = null;
                if (!notFieldFilters.isEmpty()) {
                    notFilters = Arrays.asList(
                        new MiruFilter(MiruFilterOperation.pButNotQ,
                            true,
                            notFieldFilters,
                            null));
                }

                ImmutableMap<String, MiruFilter> stumptownFilters = ImmutableMap.of(
                    "stumptown",
                    new MiruFilter(MiruFilterOperation.and,
                        false,
                        fieldFilters,
                        notFilters));

                @SuppressWarnings("unchecked")
                MiruResponse<StumptownAnswer> analyticsResponse = requestHelper.executeRequest(
                    new MiruRequest<>(tenantId, MiruActorId.NOT_PROVIDED, MiruAuthzExpression.NOT_PROVIDED,
                        new StumptownQuery(
                            new MiruTimeRange(packLookbackTime, packCeilingTime),
                            input.buckets,
                            input.messageCount,
                            MiruFilter.NO_FILTER,
                            stumptownFilters),
                        MiruSolutionLogLevel.NONE),
                    StumptownConstants.STUMPTOWN_PREFIX + StumptownConstants.CUSTOM_QUERY_ENDPOINT, MiruResponse.class,
                    new Class[] { StumptownAnswer.class },
                    null);
                response = analyticsResponse;
                if (response != null && response.answer != null) {
                    break;
                } else {
                    log.warn("Empty stumptown response from {}, trying another", requestHelper);
                }
            } catch (Exception e) {
                log.warn("Failed stumptown request to {}, trying another", new Object[] { requestHelper }, e);
            }
        }

        if (response != null && response.answer != null) {
            final long startBucketIndex = packLookbackTime / millisPerBucket;
            data.put("startBucketIndex", String.valueOf(startBucketIndex));
            data.put("elapse", String.valueOf(response.totalElapsed));

            Map<String, StumptownAnswer.Waveform> waveforms = response.answer.waveforms;
            if (waveforms == null) {
                waveforms = Collections.emptyMap();
            }

            Map<String, Object> waveformData = Maps.newHashMap();
            for (Map.Entry<String, StumptownAnswer.Waveform> entry : waveforms.entrySet()) {
                long[] waveform = entry.getValue().waveform;
                int[] counts = new int[waveform.length];
                for (int i = 0; i < counts.length; i++) {
                    counts[i] = (int) Math.min(waveform[i], Integer.MAX_VALUE);
                }
                waveformData.put(entry.getKey(), counts);
            }
            data.put("waveforms", waveformData);
            data.put("fromAgoSecs", fromTimeUnit.toSeconds(fromAgo));
            data.put("toAgoSecs", toTimeUnit.toSeconds(toAgo));


            List<Long> activityTimes = Lists.newArrayList();
            for (StumptownAnswer.Waveform waveform : waveforms.values()) {
                for (MiruActivity activity : waveform.results) {
                    activityTimes.add(activity.time);
                }
            }
            List<MiruLogEvent> logEvents = Lists.newArrayList(payloads.multiGet(tenantId, activityTimes, MiruLogEvent.class));
            Collections.reverse(logEvents);
            if (!logEvents.isEmpty()) {
                data.put("logEvents", Lists.transform(logEvents, new Function<MiruLogEvent, String>() {
                    @Override
                    public String apply(MiruLogEvent input) {
                        return renderer.render(logEventTemplate, ImmutableMap.of("event", ImmutableMap.<String, Object>builder()
                            .put("datacenter", firstNonNull(input.datacenter, ""))
                            .put("cluster", firstNonNull(input.cluster, ""))
                            .put("host", firstNonNull(input.host, ""))
                            .put("service", firstNonNull(input.service, ""))
                            .put("instance", firstNonNull(input.instance, ""))
                            .put("version", firstNonNull(input.version, ""))
                            .put("level", firstNonNull(input.level, ""))
                            .put("threadName", firstNonNull(input.threadName, ""))
                            .put("loggerName", firstNonNull(input.loggerName, ""))
                            .put("method", firstNonNull(input.methodName, ""))
                            .put("line", firstNonNull(input.lineNumber, ""))
                            .put("message", firstNonNull(input.message, ""))
                            .put("timestamp", input.timestamp != null
                                ? new ISO8601DateFormat(TimeZone.getDefault()).format(new Date(Long.parseLong(input.timestamp)))
                                : "")
                            .put("exceptionClass", firstNonNull(input.exceptionClass, ""))
                            .put("thrownStackTrace", input.thrownStackTrace != null ? Arrays.asList(input.thrownStackTrace) : Arrays.asList())
                            .build()));
                    }
                }));
            } else {
                data.put("logEvents", Arrays.asList(renderer.render(noEventsTemplate, Collections.<String, Object>emptyMap())));
            }
        }

        return data;
    }

    private void addFieldFilter(List<MiruFieldFilter> fieldFilters, List<MiruFieldFilter> notFilters, String fieldName, String values) {
        if (values != null) {
            values = values.trim();
            String[] valueArray = values.split("\\s*,\\s*");
            List<String> terms = Lists.newArrayList();
            List<String> notTerms = Lists.newArrayList();
            for (String value : valueArray) {
                String trimmed = value.trim();
                if (!trimmed.isEmpty()) {
                    if (trimmed.startsWith("!")) {
                        if (trimmed.length() > 1) {
                            notTerms.add(trimmed.substring(1));
                        }
                    } else {
                        terms.add(trimmed);
                    }
                }
            }
            if (!terms.isEmpty()) {
                fieldFilters.add(new MiruFieldFilter(MiruFieldType.primary, fieldName, terms));
            }
            if (!notTerms.isEmpty()) {
                notFilters.add(new MiruFieldFilter(MiruFieldType.primary, fieldName, notTerms));
            }
        }
    }

    @Override
    public String getTitle() {
        return "Query";
    }
}
