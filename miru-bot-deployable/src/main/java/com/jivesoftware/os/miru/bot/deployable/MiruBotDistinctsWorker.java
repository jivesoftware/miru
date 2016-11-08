package com.jivesoftware.os.miru.bot.deployable;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Charsets;
import com.jivesoftware.os.jive.utils.ordered.id.JiveEpochTimestampProvider;
import com.jivesoftware.os.jive.utils.ordered.id.OrderIdProvider;
import com.jivesoftware.os.jive.utils.ordered.id.SnowflakeIdPacker;
import com.jivesoftware.os.miru.api.MiruActorId;
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
import com.jivesoftware.os.routing.bird.shared.NextClientStrategy;
import com.jivesoftware.os.miru.bot.deployable.MiruBotDistinctsInitializer.MiruBotDistinctsConfig;
import com.jivesoftware.os.miru.bot.deployable.StatedMiruValue.State;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

class MiruBotDistinctsWorker implements Runnable {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    private final String miruIngressEndpoint;
    private final MiruBotDistinctsConfig miruBotDistinctsConfig;
    private final OrderIdProvider orderIdProvider;
    private final MiruBotSchemaService miruBotSchemaService;
    private final TenantAwareHttpClient<String> miruReaderClient;
    private final TenantAwareHttpClient<String> miruWriterClient;

    private final NextClientStrategy nextClientStrategy = new RoundRobinStrategy();
    private final Random RAND = new Random();

    private MiruBotBucket miruBotBucket;
    private AtomicBoolean running = new AtomicBoolean(false);
    private ObjectMapper objectMapper = new ObjectMapper();
    private HttpResponseMapper httpResponseMapper = new HttpResponseMapper(objectMapper);

    MiruBotDistinctsWorker(String miruIngressEndpoint,
                           MiruBotDistinctsConfig miruBotDistinctsConfig,
                           OrderIdProvider orderIdProvider,
                           MiruBotSchemaService miruBotSchemaService,
                           TenantAwareHttpClient<String> miruReaderClient,
                           TenantAwareHttpClient<String> miruWriterClient) {
        this.miruIngressEndpoint = miruIngressEndpoint;
        this.miruBotDistinctsConfig = miruBotDistinctsConfig;
        this.orderIdProvider = orderIdProvider;
        this.miruBotSchemaService = miruBotSchemaService;
        this.miruReaderClient = miruReaderClient;
        this.miruWriterClient = miruWriterClient;
    }

    public void run() {
        long start = System.currentTimeMillis();
        running.set(true);

        MiruTenantId miruTenantId = new MiruTenantId(
                ("mirubot-distincts-" + UUID.randomUUID().toString()).getBytes(Charsets.UTF_8));
        LOG.info("Miru Tenant Id: {}", miruTenantId.toString());

        miruBotBucket = new MiruBotBucket(
                miruBotDistinctsConfig.getNumberOfFields(),
                miruBotDistinctsConfig.getValueSizeFactor(),
                miruBotDistinctsConfig.getBirthRateFactor());

        int seedCount = miruBotDistinctsConfig.getBotBucketSeed();

        while (running.get()) {
            try {
                MiruSchema miruSchema = miruBotBucket.genSchema(miruTenantId);
                miruBotSchemaService.ensureSchema(miruTenantId, miruSchema);

                StatedMiruValueWriter statedMiruValueWriter =
                        new StatedMiruValueWriter(
                                miruIngressEndpoint,
                                miruBotDistinctsConfig,
                                miruWriterClient,
                                orderIdProvider);

                if (seedCount > 0) {
                    List<Map<String, StatedMiruValue>> miruSeededActivities = miruBotBucket.seed(seedCount);
                    statedMiruValueWriter.write(miruTenantId, miruSeededActivities);

                    seedCount = 0;
                    LOG.info("Wrote {} seeded activities.", miruSeededActivities.size());
                }

                while (running.get()) {
                    int count = statedMiruValueWriter.writeAll(
                            miruBotBucket,
                            miruTenantId,
                            smv -> smv.state != State.READ_FAIL);
                    LOG.info("Wrote {} activities.", count);

                    LOG.info("Sleep {}ms between writes and reads", miruBotDistinctsConfig.getWriteReadPauseMs());
                    Thread.sleep(miruBotDistinctsConfig.getWriteReadPauseMs());

                    SnowflakeIdPacker snowflakeIdPacker = new SnowflakeIdPacker();
                    long packCurrentTime = snowflakeIdPacker.pack(
                            new JiveEpochTimestampProvider().getTimestamp(), 0, 0);
                    long randTimeRange = RAND.nextInt(miruBotDistinctsConfig.getReadTimeRangeFactor());
                    long packFromTime = packCurrentTime - snowflakeIdPacker.pack(
                            randTimeRange, 0, 0);
                    MiruTimeRange miruTimeRange = new MiruTimeRange(packFromTime, packCurrentTime);
                    LOG.debug("Read from {}ms in the past until now.", randTimeRange);
                    LOG.debug("Read time range: {}", miruTimeRange);

                    LOG.debug("Query miru distincts for each field in the schema.");
                    for (MiruFieldDefinition miruFieldDefinition : miruSchema.getFieldDefinitions()) {
                        List<StatedMiruValue> distinctValuesForField =
                                miruBotBucket.getActivitiesForField(miruFieldDefinition);
                        LOG.debug("{} distinct mirubot values for {}.",
                                distinctValuesForField.size(), miruFieldDefinition.name);
                        LOG.debug("{}", distinctValuesForField);

                        DistinctsAnswer miruDistinctsAnswer = miruReadDistinctsAnswer(
                                miruTenantId,
                                miruTimeRange,
                                miruFieldDefinition);

                        if (miruDistinctsAnswer == null) {
                            LOG.error("No distincts answer (null) found for {}", miruTenantId);
                            distinctValuesForField.forEach(smv -> smv.state = State.READ_FAIL);
                        } else if (miruDistinctsAnswer.collectedDistincts == 0) {
                            LOG.error("No distincts answer (collectedDistincts=0) found for {}", miruTenantId);
                            distinctValuesForField.forEach(smv -> smv.state = State.READ_FAIL);
                        } else {
                            LOG.debug("{} distinct miru values for {}.",
                                    miruDistinctsAnswer.collectedDistincts, miruFieldDefinition.name);
                            LOG.debug("{}", miruDistinctsAnswer.results);

                            LOG.info("Number of {} miru distincts {}; mirubot {}",
                                    miruFieldDefinition.name, miruDistinctsAnswer.collectedDistincts, distinctValuesForField.size());

                            if (miruDistinctsAnswer.collectedDistincts == miruDistinctsAnswer.results.size()) {
                                if (distinctValuesForField.size() != miruDistinctsAnswer.results.size()) {
                                    LOG.warn("Number of distinct activities from miru {} not equal to mirubot {}",
                                            miruDistinctsAnswer.results.size(),
                                            distinctValuesForField.size());
                                }

                                distinctValuesForField.forEach(smv -> smv.state = State.READ_FAIL);
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
                                        LOG.warn("Did not find matching mirubot value for miru value. {}:{}", miruFieldDefinition.name, mv.last());
                                        miruBotBucket.addFieldValue(miruFieldDefinition, mv, State.READ_FAIL);
                                    }
                                });

                                if (distinctValuesForField.size() == 0) {
                                    LOG.debug("Distinct activities in miru matches mirubot.");
                                } else {
                                    LOG.warn("Distinct activities in miru does not match mirubot. {}", distinctValuesForField);
                                }
                            } else {
                                throw new RuntimeException("Distincts answer collectedDistincts not equal to results list size.");
                            }
                        }
                    }

                    long elapsed = System.currentTimeMillis() - start;
                    LOG.debug("Elapsed time {}ms", elapsed);
                    if (elapsed > miruBotDistinctsConfig.getRuntimeMs()) {
                        LOG.info("Completed after {}ms ({})", elapsed, miruBotDistinctsConfig.getRuntimeMs());
                        running.set(false);
                    }
                }
            } catch (Exception e) {
                LOG.error("Error occurred running distincts service. {}", e.getLocalizedMessage());

                try {
                    if (miruBotDistinctsConfig.getFailureRetryWaitMs() > 0) {
                        LOG.error("Sleeping {}ms before retrying.", miruBotDistinctsConfig.getFailureRetryWaitMs());
                        Thread.sleep(miruBotDistinctsConfig.getFailureRetryWaitMs());
                    }
                } catch (InterruptedException ie) {
                    LOG.error("Interrupted while sleeping on retry in distincts service. {}", ie.getLocalizedMessage());

                    running.set(false);
                    LOG.error("Stopping distincts service");
                }
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
                nextClientStrategy,
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

    void setRunning(boolean r) {
        this.running.set(r);
    }

    MiruBotBucketSnapshot genMiruBotBucketSnapshot() {
        return miruBotBucket.genSnapshot();
    }

    double getHealthPercentage() {
        double totalFail = miruBotBucket.getFieldsValuesCount(State.READ_FAIL);
        if (totalFail == 0.0) return 1.0;

        double total = miruBotBucket.getFieldsValuesCount();

        //
        // need to tweak math
        // current idea is to weight failures more than successes
        // scaled to lower 40% of health
        //

        double fuzzyReadFailCount = totalFail * 10 / total / 0.4;
        return Math.max(1.0 - fuzzyReadFailCount, 0.0);
    }

    String getHealthDescription() {
        String fail = miruBotBucket.getFieldsValues(State.READ_FAIL);
        if (fail.isEmpty()) return "";
        return "Distincts read failures: " + fail;
    }

}
