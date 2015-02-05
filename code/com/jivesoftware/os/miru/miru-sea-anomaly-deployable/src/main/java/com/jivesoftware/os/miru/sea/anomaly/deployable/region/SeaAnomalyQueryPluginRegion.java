package com.jivesoftware.os.miru.sea.anomaly.deployable.region;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.google.common.base.Function;
import com.google.common.base.Joiner;
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
import com.jivesoftware.os.miru.metric.sampler.MiruMetricSampleEvent;
import com.jivesoftware.os.miru.plugin.solution.MiruRequest;
import com.jivesoftware.os.miru.plugin.solution.MiruResponse;
import com.jivesoftware.os.miru.plugin.solution.MiruSolutionLogLevel;
import com.jivesoftware.os.miru.plugin.solution.MiruTimeRange;
import com.jivesoftware.os.miru.sea.anomaly.deployable.MiruSoyRenderer;
import com.jivesoftware.os.miru.sea.anomaly.deployable.SeaAnomalySchemaConstants;
import com.jivesoftware.os.miru.sea.anomaly.deployable.endpoints.MinMaxDouble;
import com.jivesoftware.os.miru.sea.anomaly.deployable.storage.MiruSeaAnomalyPayloads;
import com.jivesoftware.os.miru.sea.anomaly.plugins.SeaAnomalyAnswer;
import com.jivesoftware.os.miru.sea.anomaly.plugins.SeaAnomalyConstants;
import com.jivesoftware.os.miru.sea.anomaly.plugins.SeaAnomalyQuery;
import com.jivesoftware.os.mlogger.core.ISO8601DateFormat;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;

import static com.google.common.base.Objects.firstNonNull;

/**
 *
 */
// soy.sea.anomaly.page.seaAnomalyQueryPluginRegion
public class SeaAnomalyQueryPluginRegion implements PageRegion<Optional<SeaAnomalyQueryPluginRegion.SeaAnomalyPluginRegionInput>> {

    private static final MetricLogger log = MetricLoggerFactory.getLogger();

    private final String template;
    private final MiruSoyRenderer renderer;
    private final RequestHelper[] miruReaders;
    private final MiruSeaAnomalyPayloads payloads;

    public SeaAnomalyQueryPluginRegion(String template,
        MiruSoyRenderer renderer,
        RequestHelper[] miruReaders,
        MiruSeaAnomalyPayloads payloads) {

        this.template = template;
        this.renderer = renderer;
        this.miruReaders = miruReaders;
        this.payloads = payloads;
    }

    public static class SeaAnomalyPluginRegionInput {

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
        final String message;
        final String thrown;

        final int buckets;
        final int messageCount;

        public SeaAnomalyPluginRegionInput(String cluster,
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
            String message,
            String thrown,
            int buckets,
            int messageCount) {

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
            this.message = message;
            this.thrown = thrown;
            this.buckets = buckets;
            this.messageCount = messageCount;
        }

    }

    @Override
    public String render(Optional<SeaAnomalyPluginRegionInput> optionalInput) {
        Map<String, Object> data = Maps.newHashMap();
        try {
            if (optionalInput.isPresent()) {
                SeaAnomalyPluginRegionInput input = optionalInput.get();
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
                data.put("message", input.message);
                data.put("thrown", input.thrown);

                Set<String> logLevelSet = Sets.newHashSet(Splitter.on(',').split(input.logLevel));
                data.put("logLevels", ImmutableMap.of(
                    "trace", logLevelSet.contains("TRACE"),
                    "debug", logLevelSet.contains("DEBUG"),
                    "info", logLevelSet.contains("INFO"),
                    "warn", logLevelSet.contains("WARN"),
                    "error", logLevelSet.contains("ERROR")));
                data.put("fromAgo", String.valueOf(fromAgo));
                data.put("toAgo", String.valueOf(toAgo));
                data.put("buckets", String.valueOf(input.buckets));
                data.put("messageCount", String.valueOf(input.messageCount));

                SnowflakeIdPacker snowflakeIdPacker = new SnowflakeIdPacker();
                long jiveCurrentTime = new JiveEpochTimestampProvider().getTimestamp();
                final long packCurrentTime = snowflakeIdPacker.pack(jiveCurrentTime, 0, 0);
                final long fromTime = packCurrentTime - snowflakeIdPacker.pack(TimeUnit.valueOf(input.fromTimeUnit).toMillis(fromAgo), 0, 0);
                final long toTime = packCurrentTime - snowflakeIdPacker.pack(TimeUnit.valueOf(input.toTimeUnit).toMillis(toAgo), 0, 0);

                MiruTenantId tenantId = SeaAnomalySchemaConstants.TENANT_ID;
                MiruResponse<SeaAnomalyAnswer> response = null;
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
                        addFieldFilter(fieldFilters, notFieldFilters, "logger", input.logger);
                        addFieldFilter(fieldFilters, notFieldFilters, "message", input.message.toLowerCase());
                        addFieldFilter(fieldFilters, notFieldFilters, "level", input.logLevel);
                        addFieldFilter(fieldFilters, notFieldFilters, "thrownStackTrace", input.thrown.toLowerCase());

                        List<MiruFilter> notFilters = null;
                        if (!notFieldFilters.isEmpty()) {
                            notFilters = Arrays.asList(
                                new MiruFilter(MiruFilterOperation.pButNotQ,
                                    true,
                                    notFieldFilters,
                                    null));
                        }

                        ImmutableMap<String, MiruFilter> seaAnomalyFilters = ImmutableMap.of(
                            "seaAnomaly",
                            new MiruFilter(MiruFilterOperation.and,
                                false,
                                fieldFilters,
                                notFilters));

                        @SuppressWarnings("unchecked")
                        MiruResponse<SeaAnomalyAnswer> analyticsResponse = requestHelper.executeRequest(
                            new MiruRequest<>(tenantId, MiruActorId.NOT_PROVIDED, MiruAuthzExpression.NOT_PROVIDED,
                                new SeaAnomalyQuery(
                                    new MiruTimeRange(fromTime, toTime),
                                    input.buckets,
                                    input.messageCount,
                                    MiruFilter.NO_FILTER,
                                    seaAnomalyFilters),
                                MiruSolutionLogLevel.INFO), //TODO MiruSolutionLogLevel.valueOf(input.solutionLogLevel)),
                            SeaAnomalyConstants.SEA_ANOMALY_PREFIX + SeaAnomalyConstants.CUSTOM_QUERY_ENDPOINT, MiruResponse.class,
                            new Class[]{SeaAnomalyAnswer.class},
                            null);
                        response = analyticsResponse;
                        if (response != null && response.answer != null) {
                            break;
                        } else {
                            log.warn("Empty seaAnomaly response from {}, trying another", requestHelper);
                        }
                    } catch (Exception e) {
                        log.warn("Failed seaAnomaly request to {}, trying another", new Object[]{requestHelper}, e);
                    }
                }

                if (response != null && response.answer != null) {
                    Map<String, SeaAnomalyAnswer.Waveform> waveforms = response.answer.waveforms;
                    if (waveforms == null) {
                        waveforms = Collections.emptyMap();
                    }
                    data.put("elapse", String.valueOf(response.totalElapsed));

                    Map<String, long[]> rawWaveforms = new HashMap<>();
                    for (Entry<String, SeaAnomalyAnswer.Waveform> e : waveforms.entrySet()) {
                        rawWaveforms.put(e.getKey(), e.getValue().waveform);
                    }

                    data.put("waveform", "data:image/png;base64," + new PNGWaveforms().hitsToBase64PNGWaveform(1024, 200, 32, 10, rawWaveforms,
                        Optional.<MinMaxDouble>absent()));

                    List<Long> activityTimes = Lists.newArrayList();
                    for (SeaAnomalyAnswer.Waveform waveform : waveforms.values()) {
                        for (MiruActivity activity : waveform.results) {
                            activityTimes.add(activity.time);
                        }
                    }
                    List<MiruMetricSampleEvent> logEvents = payloads.multiGet(tenantId, activityTimes, MiruMetricSampleEvent.class);
                    /*
                     List<MiruLogEvent> logEvents = Arrays.asList(
                     new MiruLogEvent("dc", "clu", "host", "serv", "inst", "ver", "INFO", "t-1", "c.m.j.s.Class", "hello",
                     String.valueOf(System.currentTimeMillis()), new String[] { "a", "b", "c" }));
                     */
                    data.put("events", Lists.transform(logEvents, new Function<MiruMetricSampleEvent, Map<String, Object>>() {
                        @Override
                        public Map<String, Object> apply(MiruMetricSampleEvent input) {
                            return ImmutableMap.<String, Object>builder()
                                .put("datacenter", firstNonNull(input.datacenter, ""))
                                .put("cluster", firstNonNull(input.cluster, ""))
                                .put("host", firstNonNull(input.host, ""))
                                .put("service", firstNonNull(input.service, ""))
                                .put("instance", firstNonNull(input.instance, ""))
                                .put("version", firstNonNull(input.version, ""))
                                .put("timestamp", input.timestamp != null
                                        ? new ISO8601DateFormat(TimeZone.getDefault()).format(new Date(Long.parseLong(input.timestamp)))
                                        : "")
                                .build();
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
