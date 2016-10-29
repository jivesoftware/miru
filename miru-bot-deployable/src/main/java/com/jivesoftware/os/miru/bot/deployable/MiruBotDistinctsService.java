package com.jivesoftware.os.miru.bot.deployable;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Charsets;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.jivesoftware.os.jive.utils.ordered.id.JiveEpochTimestampProvider;
import com.jivesoftware.os.jive.utils.ordered.id.OrderIdProvider;
import com.jivesoftware.os.jive.utils.ordered.id.SnowflakeIdPacker;
import com.jivesoftware.os.miru.api.MiruActorId;
import com.jivesoftware.os.miru.api.activity.MiruActivity;
import com.jivesoftware.os.miru.api.activity.schema.MiruFieldDefinition;
import com.jivesoftware.os.miru.api.activity.schema.MiruSchema;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.api.query.filter.MiruAuthzExpression;
import com.jivesoftware.os.miru.api.query.filter.MiruFilter;
import com.jivesoftware.os.miru.plugin.solution.MiruRequest;
import com.jivesoftware.os.miru.plugin.solution.MiruResponse;
import com.jivesoftware.os.miru.plugin.solution.MiruSolutionLogLevel;
import com.jivesoftware.os.miru.plugin.solution.MiruTimeRange;
import com.jivesoftware.os.miru.reco.plugins.distincts.DistinctsAnswer;
import com.jivesoftware.os.miru.reco.plugins.distincts.DistinctsConstants;
import com.jivesoftware.os.miru.reco.plugins.distincts.DistinctsQuery;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import com.jivesoftware.os.routing.bird.http.client.HttpResponse;
import com.jivesoftware.os.routing.bird.http.client.HttpResponseMapper;
import com.jivesoftware.os.routing.bird.http.client.RoundRobinStrategy;
import com.jivesoftware.os.routing.bird.http.client.TenantAwareHttpClient;
import com.jivesoftware.os.routing.bird.shared.ClientCall;
import com.jivesoftware.os.miru.bot.deployable.MiruBotDistinctsInitializer.MiruBotDistinctsConfig;
import com.jivesoftware.os.miru.bot.deployable.StatedMiruValue.State;
import com.jivesoftware.os.routing.bird.shared.NextClientStrategy;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;

class MiruBotDistinctsService implements MiruBotHealthPercent {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    private final MiruBotDistinctsConfig miruBotDistinctsConfig;
    private final String miruIngressEndpoint;
    private final ObjectMapper objectMapper;
    private final HttpResponseMapper httpResponseMapper;
    private final OrderIdProvider orderIdProvider;
    private final MiruBotSchemaService miruBotSchemaService;
    private final TenantAwareHttpClient<String> miruReaderClient;
    private final TenantAwareHttpClient<String> miruWriterClient;

    private MiruBotBucket miruBotBucket;

    private final AtomicBoolean running = new AtomicBoolean(false);
    private final ScheduledExecutorService processor =
            Executors.newSingleThreadScheduledExecutor(
                    new ThreadFactoryBuilder().setNameFormat("mirubot-distincts-%d").build());

    private final NextClientStrategy nextClientStrategyReader = new RoundRobinStrategy();
    private final NextClientStrategy nextClientStrategyWriter = new RoundRobinStrategy();

    private final Random RAND = new Random();

    MiruBotDistinctsService(String miruIngressEndpoint,
                            MiruBotDistinctsConfig miruBotDistinctsConfig,
                            ObjectMapper objectMapper,
                            HttpResponseMapper httpResponseMapper,
                            OrderIdProvider orderIdProvider,
                            MiruBotSchemaService miruBotSchemaService,
                            TenantAwareHttpClient<String> miruReaderClient,
                            TenantAwareHttpClient<String> miruWriterClient) {
        this.miruIngressEndpoint = miruIngressEndpoint;
        this.miruBotDistinctsConfig = miruBotDistinctsConfig;
        this.objectMapper = objectMapper;
        this.httpResponseMapper = httpResponseMapper;
        this.orderIdProvider = orderIdProvider;
        this.miruBotSchemaService = miruBotSchemaService;
        this.miruReaderClient = miruReaderClient;
        this.miruWriterClient = miruWriterClient;

        miruBotBucket = new MiruBotBucket(miruBotDistinctsConfig);
    }

    void start() {
        LOG.info("Enabled: {}", miruBotDistinctsConfig.getEnabled());
        LOG.info("Read time range factor: {}ms", miruBotDistinctsConfig.getReadTimeRangeFactorMs());
        LOG.info("Write hesitation factor: {}ms", miruBotDistinctsConfig.getWriteHesitationFactorMs());
        LOG.info("Value size factor: {}", miruBotDistinctsConfig.getValueSizeFactor());
        //LOG.info("Retry wait: {}", miruBotDistinctsConfig.getRetryWaitMs());
        LOG.info("Birth rate factor: {}", miruBotDistinctsConfig.getBirthRateFactor());
        LOG.info("Read frequency: {}", miruBotDistinctsConfig.getReadFrequency());
        LOG.info("Batch write count factor: {}", miruBotDistinctsConfig.getBatchWriteCountFactor());
        LOG.info("Batch write frequency: {}", miruBotDistinctsConfig.getBatchWriteFrequency());
        LOG.info("Number of fields: {}", miruBotDistinctsConfig.getNumberOfFields());
        LOG.info("Bot bucket seed: {}", miruBotDistinctsConfig.getBotBucketSeed());
        LOG.info("Write read pause: {}ms", miruBotDistinctsConfig.getWriteReadPauseMs());

        if (!miruBotDistinctsConfig.getEnabled()) {
            LOG.warn("Not starting distincts service; not enabled.");
            return;
        }

        running.set(true);

        processor.submit(() -> {
            MiruTenantId miruTenantId = new MiruTenantId(
                    ("mirubot-" + UUID.randomUUID().toString()).getBytes(Charsets.UTF_8));

            MiruSchema miruSchema = miruBotBucket.genSchema();
            miruBotSchemaService.ensureSchema(miruTenantId, miruSchema);
            List<Map<String, StatedMiruValue>> miruSeededActivities =
                    miruBotBucket.seed(miruBotDistinctsConfig.getBotBucketSeed());
            miruWriteActivities(miruTenantId, miruSeededActivities);
            LOG.info("Wrote {} seeded activities.", miruSeededActivities.size());

            while (running.get()) {
                try {
                    for (int i = 0; i < miruBotDistinctsConfig.getReadFrequency();) {
                        List<Map<String, StatedMiruValue>> fieldsValues = Lists.newArrayList();

                        int activityCount = 1;
                        if ((i + 1) % miruBotDistinctsConfig.getBatchWriteFrequency() == 0) {
                            activityCount = RAND.nextInt(miruBotDistinctsConfig.getBatchWriteCountFactor());
                        }

                        for (int j = 0; j < activityCount; j++) {
                            fieldsValues.add(miruBotBucket.genWriteMiruActivity());
                            i++;
                        }

                        miruWriteActivities(miruTenantId, fieldsValues);
                        LOG.info("Wrote {} activities.", fieldsValues.size());

                        if (miruBotDistinctsConfig.getWriteHesitationFactorMs() > 0) {
                            Thread.sleep(RAND.nextInt(miruBotDistinctsConfig.getWriteHesitationFactorMs()));
                        }
                    }

                    LOG.info("Sleep {}ms between writes and reads", miruBotDistinctsConfig.getWriteReadPauseMs());
                    Thread.sleep(miruBotDistinctsConfig.getWriteReadPauseMs());

                    SnowflakeIdPacker snowflakeIdPacker = new SnowflakeIdPacker();
                    long packCurrentTime = snowflakeIdPacker.pack(
                            new JiveEpochTimestampProvider().getTimestamp(), 0, 0);
                    long randTimeRange = RAND.nextInt(miruBotDistinctsConfig.getReadTimeRangeFactorMs());
                    long packFromTime = packCurrentTime - snowflakeIdPacker.pack(
                            randTimeRange, 0, 0);
                    MiruTimeRange miruTimeRange = new MiruTimeRange(packFromTime, packCurrentTime);
                    LOG.debug("Read from {}ms in the past until now.", randTimeRange);
                    LOG.debug("Read time range: {}", miruTimeRange);

                    LOG.debug("Query miru distincts for each field in the schema.");
                    for (MiruFieldDefinition miruFieldDefinition : miruSchema.getFieldDefinitions()) {
                        List<StatedMiruValue> distinctValuesForField =
                                miruBotBucket.getActivitiesForField(miruFieldDefinition);
                        LOG.info("{} distinct mirubot values for {}.", distinctValuesForField.size(), miruFieldDefinition.name);
                        LOG.info("{}", distinctValuesForField);

                        DistinctsAnswer miruDistinctsAnswer = miruReadDistinctsAnswer(
                                miruTenantId,
                                miruTimeRange,
                                miruFieldDefinition);

                        if (miruDistinctsAnswer == null) {
                            LOG.error("No distincts answer (null) found for {}", miruTenantId);
                            distinctValuesForField.forEach((smv) -> smv.state = State.READ_FAIL);
                        } else if (miruDistinctsAnswer.collectedDistincts == 0) {
                            LOG.error("No distincts answer (collectedDistincts=0) found for {}", miruTenantId);
                            distinctValuesForField.forEach((smv) -> smv.state = State.READ_FAIL);
                        } else {
                            LOG.info("{} distinct miru values for {}.", miruDistinctsAnswer.collectedDistincts, miruFieldDefinition.name);
                            LOG.info("{}", miruDistinctsAnswer.results);

                            if (miruDistinctsAnswer.collectedDistincts == miruDistinctsAnswer.results.size()) {
                                if (distinctValuesForField.size() != miruDistinctsAnswer.results.size()) {
                                    LOG.warn("Number of distinct activities from miru {} not equal to mirubot {}",
                                            miruDistinctsAnswer.results.size(),
                                            distinctValuesForField.size());
                                }

                                distinctValuesForField.forEach((smv) -> smv.state = State.READ_FAIL);
                                miruDistinctsAnswer.results.forEach((mv) -> {
                                    Optional<StatedMiruValue> statedMiruValue = distinctValuesForField
                                            .stream()
                                            .filter(smv -> mv.equals(smv.value))
                                            .findFirst();
                                    if (statedMiruValue.isPresent()) {
                                        LOG.debug("Found matching value from miru to mirubot. {}:{}", miruFieldDefinition.name, mv.last());
                                        statedMiruValue.get().state = State.READ_SUCCESS;
                                        distinctValuesForField.remove(statedMiruValue.get());
                                    } else {
                                        LOG.error("Did not find matching mirubot value for miru value. {}:{}", miruFieldDefinition.name, mv.last());
                                        miruBotBucket.addFieldValue(miruFieldDefinition, mv, State.READ_FAIL);
                                    }
                                });

                                if (distinctValuesForField.size() == 0) {
                                    LOG.info("Distinct activities in miru matches mirubot.");
                                } else {
                                    LOG.error("Distinct activities in miru does not match mirubot. {}", distinctValuesForField);
                                }
                            } else {
                                throw new RuntimeException("Distincts answer collectedDistincts not equal to results list size.");
                            }
                        }
                    }
                } catch (Exception e) {
                    LOG.error("Error occurred running distincts service. {}", e.getLocalizedMessage());
                    running.set(false);
                }
            }

            return null;
        });
    }

    public void stop() throws InterruptedException {
        running.set(false);
        Thread.sleep(miruBotDistinctsConfig.getWriteHesitationFactorMs());

        processor.shutdownNow();
    }

    private void miruWriteActivities(MiruTenantId miruTenantId,
                                     List<Map<String, StatedMiruValue>> fieldsStatedValues) throws Exception {
        LOG.debug("Miru write {} activities", fieldsStatedValues.size());

        List<MiruActivity> miruActivities = Lists.newArrayList();
        for (Map<String, StatedMiruValue> fieldValue : fieldsStatedValues) {
            StringBuilder sb = new StringBuilder();
            Map<String, List<String>> fieldsValues = Maps.newHashMap();

            for (Entry<String, StatedMiruValue> value : fieldValue.entrySet()) {
                sb.append(value.getKey());
                sb.append("-");
                sb.append(value.getValue().value.last());
                sb.append(",");

                fieldsValues.put(value.getKey(), Collections.singletonList(value.getValue().value.last()));
            }

            LOG.debug("Write miru activity: " + sb.toString());
            miruActivities.add(new MiruActivity(
                    miruTenantId,
                    orderIdProvider.nextId(),
                    0,
                    false,
                    new String[0],
                    fieldsValues,
                    Collections.emptyMap()));
        }

        String jsonActivities = objectMapper.writeValueAsString(miruActivities);
        LOG.debug("Miru activity json post data '{}'", jsonActivities);

        HttpResponse httpResponse = miruWriterClient.call(
                "",
                nextClientStrategyWriter,
                "ingress",
                client -> new ClientCall.ClientResponse<>(client.postJson(miruIngressEndpoint, jsonActivities, null), true));

        LOG.debug("Miru write response {}:{}", httpResponse.getStatusCode(), new String(httpResponse.getResponseBody()));
        if (httpResponse.getStatusCode() < 200 || httpResponse.getStatusCode() >= 300) {
            throw new RuntimeException("Failed to post activities to " + miruIngressEndpoint);
        }

        for (Map<String, StatedMiruValue> fieldValue : fieldsStatedValues) {
            for (Entry<String, StatedMiruValue> value : fieldValue.entrySet()) {
                value.getValue().state = State.WRITTEN;
            }
        }
    }

    private DistinctsAnswer miruReadDistinctsAnswer(
            MiruTenantId miruTenantId,
            MiruTimeRange miruTimeRange,
            MiruFieldDefinition miruFieldDefinition) throws Exception {
        LOG.debug("Read distincts answer from miru");

        MiruRequest<DistinctsQuery> request = new MiruRequest<>(
                "mirubot",
                miruTenantId,
                MiruActorId.NOT_PROVIDED,
                MiruAuthzExpression.NOT_PROVIDED,
                new DistinctsQuery(
                        miruTimeRange,
                        miruFieldDefinition.name,
                        null,
                        MiruFilter.NO_FILTER,
                        null),
                MiruSolutionLogLevel.DEBUG);

        String jsonRequest = objectMapper.writeValueAsString(request);
        LOG.debug("Read distincts json {}", jsonRequest);

        String endpoint = DistinctsConstants.DISTINCTS_PREFIX + DistinctsConstants.CUSTOM_QUERY_ENDPOINT;
        LOG.debug("Read distincts endpoint {}", endpoint);

        MiruResponse<DistinctsAnswer> distinctsResponse = miruReaderClient.call(
                "",
                nextClientStrategyReader,
                "distinctsQuery",
                httpClient -> {
                    HttpResponse httpResponse = httpClient.postJson(endpoint, jsonRequest, null);
                    LOG.debug("Miru read response {}:{}", httpResponse.getStatusCode(), new String(httpResponse.getResponseBody()));

                    if (httpResponse.getStatusCode() < 200 || httpResponse.getStatusCode() >= 300) {
                        LOG.error("Error occurred reading distincts {}:{}", httpResponse.getStatusCode(), new String(httpResponse.getResponseBody()));
                        throw new RuntimeException("Failed to read distincts from " + endpoint);
                    }

                    @SuppressWarnings("unchecked")
                    MiruResponse<DistinctsAnswer> extractResponse = httpResponseMapper.extractResultFromResponse(
                            httpResponse,
                            MiruResponse.class,
                            new Class[]{DistinctsAnswer.class},
                            null);

                    return new ClientCall.ClientResponse<>(extractResponse, true);
                });

        LOG.debug("Read distincts results {}", distinctsResponse.answer.results);
        LOG.debug("Read collected distincts {}", distinctsResponse.answer.collectedDistincts);
        return distinctsResponse.answer;
    }

    public double getHealthPercentage() {
        double readFailCount = miruBotBucket.getFieldsValuesCount(State.READ_FAIL);
        if (readFailCount == 0.0) return 1.0;

        double totalCount = miruBotBucket.getFieldsValuesCount();

        double readFailCountWeighted10xsScaledTo40 = readFailCount * 10 / totalCount / 0.4;

        return Math.max(1.0 - readFailCountWeighted10xsScaledTo40, 0.0);
    }

    public String getHealthDescription() {
        String res = miruBotBucket.getFieldsValues(State.READ_FAIL);
        if (!res.isEmpty()) return "Distinct read failures: " + res;
        return "";
    }

}
