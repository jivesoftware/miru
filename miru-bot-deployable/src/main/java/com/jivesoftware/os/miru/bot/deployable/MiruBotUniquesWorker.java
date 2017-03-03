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
import com.jivesoftware.os.miru.bot.deployable.MiruBotUniquesInitializer.MiruBotUniquesConfig;
import com.jivesoftware.os.miru.bot.deployable.StatedMiruValue.State;
import com.jivesoftware.os.miru.plugin.solution.MiruRequest;
import com.jivesoftware.os.miru.plugin.solution.MiruResponse;
import com.jivesoftware.os.miru.plugin.solution.MiruSolutionLogLevel;
import com.jivesoftware.os.miru.plugin.solution.MiruTimeRange;
import com.jivesoftware.os.miru.reco.plugins.uniques.UniquesAnswer;
import com.jivesoftware.os.miru.reco.plugins.uniques.UniquesConstants;
import com.jivesoftware.os.miru.reco.plugins.uniques.UniquesQuery;
import com.jivesoftware.os.mlogger.core.AtomicCounter;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import com.jivesoftware.os.routing.bird.http.client.HttpResponse;
import com.jivesoftware.os.routing.bird.http.client.HttpResponseMapper;
import com.jivesoftware.os.routing.bird.http.client.RoundRobinStrategy;
import com.jivesoftware.os.routing.bird.http.client.TenantAwareHttpClient;
import com.jivesoftware.os.routing.bird.shared.ClientCall;
import com.jivesoftware.os.routing.bird.shared.NextClientStrategy;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

class MiruBotUniquesWorker implements Runnable {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    private final String miruIngressEndpoint;
    private final MiruBotUniquesConfig miruBotUniquesConfig;
    private final OrderIdProvider orderIdProvider;
    private final MiruBotSchemaService miruBotSchemaService;
    private final TenantAwareHttpClient<String> miruReaderClient;
    private final TenantAwareHttpClient<String> miruWriterClient;

    private final NextClientStrategy nextClientStrategy = new RoundRobinStrategy();

    private MiruBotBucket miruBotBucket;
    private AtomicBoolean running = new AtomicBoolean(false);
    private ObjectMapper objectMapper = new ObjectMapper();
    private HttpResponseMapper httpResponseMapper = new HttpResponseMapper(objectMapper);

    MiruBotUniquesWorker(String miruIngressEndpoint,
        MiruBotUniquesConfig miruBotUniquesConfig,
        OrderIdProvider orderIdProvider,
        MiruBotSchemaService miruBotSchemaService,
        TenantAwareHttpClient<String> miruReaderClient,
        TenantAwareHttpClient<String> miruWriterClient) {
        this.miruIngressEndpoint = miruIngressEndpoint;
        this.miruBotUniquesConfig = miruBotUniquesConfig;
        this.orderIdProvider = orderIdProvider;
        this.miruBotSchemaService = miruBotSchemaService;
        this.miruReaderClient = miruReaderClient;
        this.miruWriterClient = miruWriterClient;
    }

    public void run() {
        long start = System.currentTimeMillis();
        running.set(true);

        MiruTenantId miruTenantId = new MiruTenantId(
            ("mirubot-uniques-" + UUID.randomUUID().toString()).getBytes(Charsets.UTF_8));
        LOG.info("Miru Tenant Id: {}", miruTenantId.toString());

        miruBotBucket = new MiruBotBucket(
            miruBotUniquesConfig.getNumberOfFields(),
            miruBotUniquesConfig.getValueSizeFactor(),
            miruBotUniquesConfig.getBirthRateFactor());

        int seedCount = miruBotUniquesConfig.getBotBucketSeed();
        AtomicCounter totalCount = new AtomicCounter();

        while (running.get()) {
            try {
                MiruSchema miruSchema = miruBotBucket.genSchema(miruTenantId);
                miruBotSchemaService.ensureSchema(miruTenantId, miruSchema);

                StatedMiruValueWriter statedMiruValueWriter =
                    new StatedMiruValueWriter(
                        miruIngressEndpoint,
                        miruBotUniquesConfig,
                        miruWriterClient,
                        orderIdProvider);

                if (seedCount > 0) {
                    List<Map<String, StatedMiruValue>> miruSeededActivities = miruBotBucket.seed(seedCount);
                    statedMiruValueWriter.write(miruTenantId, miruSeededActivities);
                    LOG.info("Wrote {} seeded uniques activities.", seedCount);

                    totalCount.inc(seedCount);
                    seedCount = 0;
                }

                while (running.get()) {
                    AtomicCounter count = statedMiruValueWriter.writeAll(
                        miruBotBucket,
                        miruTenantId,
                        smv -> smv.state != State.READ_FAIL);
                    totalCount.inc(count.getCount());
                    LOG.info("Wrote {} of {} activities",
                        count.getCount(), totalCount.getCount());

                    LOG.info("Sleep {}ms between writes and reads", miruBotUniquesConfig.getWriteReadPauseMs());
                    Thread.sleep(miruBotUniquesConfig.getWriteReadPauseMs());

                    SnowflakeIdPacker snowflakeIdPacker = new SnowflakeIdPacker();
                    long packCurrentTime = snowflakeIdPacker.pack(
                        new JiveEpochTimestampProvider().getTimestamp(), 0, 0);
                    long packFromTime = packCurrentTime - snowflakeIdPacker.pack(
                        miruBotUniquesConfig.getReadTimeRange(), 0, 0);
                    MiruTimeRange miruTimeRange = new MiruTimeRange(packFromTime, packCurrentTime);
                    LOG.debug("Read from {}ms in the past until now.",
                        miruBotUniquesConfig.getReadTimeRange());
                    LOG.debug("Read time range: {}", miruTimeRange);

                    LOG.debug("Query miru uniques for each field in the schema.");
                    for (MiruFieldDefinition miruFieldDefinition : miruSchema.getFieldDefinitions()) {
                        List<StatedMiruValue> uniqueValuesForField = miruBotBucket.getActivitiesForField(miruFieldDefinition);
                        LOG.debug("{} unique mirubot values for {}.",
                            uniqueValuesForField.size(), miruFieldDefinition.name);

                        UniquesAnswer miruUniquesAnswer = miruReadUniquesAnswer(
                            miruTenantId,
                            miruTimeRange,
                            miruFieldDefinition);

                        uniqueValuesForField.forEach(smv -> smv.state = State.READ_FAIL);

                        if (miruUniquesAnswer == null) {
                            LOG.error("No uniques answer (null) found for {}", miruTenantId);
                        } else if (miruUniquesAnswer.uniques == 0L) {
                            LOG.error("No uniques answer (uniques=0) found for {}", miruTenantId);
                        } else {
                            LOG.debug("{} unique miru values for {}.",
                                miruUniquesAnswer.uniques, miruFieldDefinition.name);
                            LOG.info("Number of {} miru uniques {}; mirubot {}",
                                miruFieldDefinition.name, miruUniquesAnswer.uniques, uniqueValuesForField.size());

                            if (miruUniquesAnswer.uniques == uniqueValuesForField.size()) {
                                LOG.debug("Unique activities in miru matches mirubot.");
                                uniqueValuesForField.forEach(smv -> smv.state = State.READ_SUCCESS);
                            } else {
                                LOG.debug("Unique activities in mirubot {}", uniqueValuesForField);
                                LOG.warn("Unique activities in miru {} does not match mirubot {}",
                                    miruUniquesAnswer.uniques,
                                    uniqueValuesForField.size());
                            }
                        }
                    }

                    long elapsed = System.currentTimeMillis() - start;
                    LOG.debug("Elapsed time {}ms", elapsed);
                    if (elapsed > miruBotUniquesConfig.getRuntimeMs()) {
                        LOG.info("Completed after {}ms ({})", elapsed, miruBotUniquesConfig.getRuntimeMs());
                        running.set(false);
                    }
                }
            } catch (Exception e) {
                LOG.error("Error occurred running uniques service. {}", e.getLocalizedMessage());

                try {
                    if (miruBotUniquesConfig.getFailureRetryWaitMs() > 0) {
                        LOG.error("Sleeping {}ms before retrying.", miruBotUniquesConfig.getFailureRetryWaitMs());
                        Thread.sleep(miruBotUniquesConfig.getFailureRetryWaitMs());
                    }
                } catch (InterruptedException ie) {
                    LOG.error("Interrupted while sleeping on retry in uniques service. {}", ie.getLocalizedMessage());

                    running.set(false);
                    LOG.error("Stopping uniques service");
                }
            }
        }
    }

    private UniquesAnswer miruReadUniquesAnswer(
        MiruTenantId miruTenantId,
        MiruTimeRange miruTimeRange,
        MiruFieldDefinition miruFieldDefinition) throws Exception {
        LOG.debug("Read uniques answer from miru");

        MiruRequest<UniquesQuery> request = new MiruRequest<>(
            "mirubot",
            miruTenantId,
            MiruActorId.NOT_PROVIDED,
            MiruAuthzExpression.NOT_PROVIDED,
            new UniquesQuery(
                miruTimeRange,
                miruFieldDefinition.name,
                null,
                MiruFilter.NO_FILTER,
                null),
            MiruSolutionLogLevel.DEBUG);

        String jsonRequest = objectMapper.writeValueAsString(request);
        LOG.debug("Read uniques json {}", jsonRequest);

        String endpoint = UniquesConstants.UNIQUES_PREFIX + UniquesConstants.CUSTOM_QUERY_ENDPOINT;
        LOG.debug("Read uniques endpoint {}", endpoint);

        MiruResponse<UniquesAnswer> uniquesResponse = miruReaderClient.call(
            "",
            nextClientStrategy,
            "uniquesQuery",
            httpClient -> {
                HttpResponse httpResponse = httpClient.postJson(endpoint, jsonRequest, null);
                LOG.debug("Miru read response {}:{}", httpResponse.getStatusCode(), new String(httpResponse.getResponseBody()));

                if (httpResponse.getStatusCode() < 200 || httpResponse.getStatusCode() >= 300) {
                    LOG.error("Error occurred reading uniques {}:{}", httpResponse.getStatusCode(), new String(httpResponse.getResponseBody()));
                    throw new RuntimeException("Failed to read uniques from " + endpoint);
                }

                @SuppressWarnings("unchecked")
                MiruResponse<UniquesAnswer> extractResponse = httpResponseMapper.extractResultFromResponse(
                    httpResponse,
                    MiruResponse.class,
                    new Class[] { UniquesAnswer.class },
                    null);

                return new ClientCall.ClientResponse<>(extractResponse, true);
            });

        LOG.debug("Read answer uniques {}", uniquesResponse.answer.uniques);
        return uniquesResponse.answer;
    }

    void setRunning(boolean r) {
        running.set(r);
    }

    MiruBotBucketSnapshot genMiruBotBucketSnapshot() {
        return miruBotBucket.genSnapshot();
    }

    double getHealthPercentage() {
        double totalFail = miruBotBucket.getFieldsValuesCount(State.READ_FAIL);
        if (totalFail == 0.0) {
            return 1.0;
        }

        double total = miruBotBucket.getFieldsValuesCount();

        return 1.0 - totalFail / total;
    }

    String getHealthDescription() {
        String fail = miruBotBucket.getFieldsValues(State.READ_FAIL);
        if (fail.isEmpty()) {
            return "";
        }
        return "Uniques read failures: " + fail;
    }

}
