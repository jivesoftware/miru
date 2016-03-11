/*
 * Copyright 2016 jonathan.colt.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.jivesoftware.os.miru.catwalk.deployable;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;
import com.jivesoftware.os.amza.api.TimestampedValue;
import com.jivesoftware.os.amza.api.partition.Consistency;
import com.jivesoftware.os.amza.api.partition.Durability;
import com.jivesoftware.os.amza.api.partition.PartitionName;
import com.jivesoftware.os.amza.api.partition.PartitionProperties;
import com.jivesoftware.os.amza.api.stream.RowType;
import com.jivesoftware.os.amza.service.AmzaService;
import com.jivesoftware.os.amza.service.EmbeddedClientProvider;
import com.jivesoftware.os.amza.service.EmbeddedClientProvider.EmbeddedClient;
import com.jivesoftware.os.miru.api.MiruActorId;
import com.jivesoftware.os.miru.api.MiruStats;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.api.query.filter.MiruAuthzExpression;
import com.jivesoftware.os.miru.plugin.query.MiruTenantQueryRouting;
import com.jivesoftware.os.miru.plugin.solution.MiruRequest;
import com.jivesoftware.os.miru.plugin.solution.MiruResponse;
import com.jivesoftware.os.miru.plugin.solution.MiruSolutionLogLevel;
import com.jivesoftware.os.miru.stream.plugins.catwalk.CatwalkAnswer;
import com.jivesoftware.os.miru.stream.plugins.catwalk.CatwalkConstants;
import com.jivesoftware.os.miru.stream.plugins.catwalk.CatwalkQuery;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import com.jivesoftware.os.routing.bird.http.client.HttpResponseMapper;
import com.jivesoftware.os.routing.bird.http.client.TenantAwareHttpClient;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * @author jonathan.colt
 */
public class CatwalkModelUpdater {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    private static final PartitionProperties PROCESSED_MODEL = new PartitionProperties(Durability.fsync_async,
        TimeUnit.DAYS.toMillis(30), TimeUnit.DAYS.toMillis(10), TimeUnit.DAYS.toMillis(30), TimeUnit.DAYS.toMillis(10), 0, 0, 0, 0,
        false, Consistency.quorum, true, true, false, RowType.primary, "berkeleydb", null, -1, -1);

    private static final PartitionProperties CATS_WALKED = new PartitionProperties(Durability.fsync_async,
        TimeUnit.DAYS.toMillis(30), TimeUnit.DAYS.toMillis(10), TimeUnit.DAYS.toMillis(30), TimeUnit.DAYS.toMillis(10), 0, 0, 0, 0,
        false, Consistency.none, true, true, false, RowType.primary, "berkeleydb", null, -1, -1);

    private final CatwalkModelService modelService;
    private final CatwalkModelQueue modelQueue;
    private final ScheduledExecutorService queueConsumers;
    private final MiruTenantQueryRouting tenantQueryRouting;
    private final TenantAwareHttpClient<String> readerClient;
    private final ObjectMapper requestMapper;
    private final HttpResponseMapper responseMapper;
    private final ExecutorService modelUpdaters;
    private final AmzaService amzaService;
    private final EmbeddedClientProvider embeddedClientProvider;
    private final MiruStats stats;
    private final long modelUpdateIntervalInMillis;

    public CatwalkModelUpdater(CatwalkModelService modelService,
        CatwalkModelQueue modelQueue,
        ScheduledExecutorService queueConsumers,
        MiruTenantQueryRouting tenantQueryRouting,
        TenantAwareHttpClient<String> readerClient,
        ObjectMapper requestMapper,
        HttpResponseMapper responseMapper,
        ExecutorService modelUpdaters,
        AmzaService amzaService,
        EmbeddedClientProvider embeddedClientProvider,
        MiruStats stats,
        long modelUpdateIntervalInMillis) {

        this.modelService = modelService;
        this.modelQueue = modelQueue;
        this.queueConsumers = queueConsumers;
        this.tenantQueryRouting = tenantQueryRouting;
        this.readerClient = readerClient;
        this.requestMapper = requestMapper;
        this.responseMapper = responseMapper;
        this.modelUpdaters = modelUpdaters;
        this.amzaService = amzaService;
        this.embeddedClientProvider = embeddedClientProvider;
        this.stats = stats;
        this.modelUpdateIntervalInMillis = modelUpdateIntervalInMillis;
    }

    public void start(int numQueues, int checkQueuesBatchSize, long checkQueuesForWorkEveryNMillis) throws Exception {
        for (int q = 0; q < numQueues; q++) {
            queueConsumers.scheduleWithFixedDelay(new ScheduledQueueConsumer(q, checkQueuesBatchSize),
                0,
                checkQueuesForWorkEveryNMillis,
                TimeUnit.MILLISECONDS);
        }
    }

    private final class ScheduledQueueConsumer implements Runnable {

        private final int queueId;
        private final int batchSize;

        public ScheduledQueueConsumer(int queueId, int batchSize) {
            this.queueId = queueId;
            this.batchSize = batchSize;
        }

        @Override
        public void run() {
            try {
                while (modelQueue.isLeader(queueId)) {
                    List<UpdateModelRequest> batch = modelQueue.getBatch(queueId, batchSize);
                    if (batch.isEmpty()) {
                        break;
                    }

                    List<Future<UpdateModelRequest>> modelUpdateFutures = Lists.newArrayList();
                    for (UpdateModelRequest request : batch) {
                        modelUpdateFutures.add(modelUpdaters.submit(() -> {
                            if (modelQueue.isLeader(queueId)) {
                                ModelFeatureScores[] models = fetchModel(request);
                                modelService.saveModel(request.tenantId,
                                    request.catwalkId,
                                    request.modelId,
                                    request.partitionId,
                                    request.partitionId,
                                    request.catwalkQuery.featureFields,
                                    models);
                                return request;
                            } else {
                                return null;
                            }
                        }));
                    }

                    List<UpdateModelRequest> processedRequests = Lists.newArrayListWithCapacity(modelUpdateFutures.size());
                    for (Future<UpdateModelRequest> future : modelUpdateFutures) {
                        UpdateModelRequest request = future.get();
                        if (request != null) {
                            processedRequests.add(request);
                        }
                    }

                    modelQueue.remove(queueId, processedRequests);

                    EmbeddedClient processedClient = processedClient(queueId);
                    processedClient.commit(Consistency.quorum,
                        null,
                        commitKeyValueStream -> {
                            for (UpdateModelRequest request : processedRequests) {
                                byte[] key = modelQueue.updateModelKey(request.tenantId, request.catwalkId, request.modelId, request.partitionId);
                                if (!commitKeyValueStream.commit(key, new byte[0], request.timestamp, false)) {
                                    return false;
                                }
                            }
                            return true;
                        },
                        10_000, // TODO config
                        TimeUnit.MILLISECONDS);
                }
            } catch (Exception x) {
                LOG.error("Unexpected issue while checking queue:{}", new Object[]{queueId}, x);
            }
        }
    }

    private ModelFeatureScores[] fetchModel(UpdateModelRequest updateModelRequest) throws Exception {
        MiruTenantId tenantId = updateModelRequest.tenantId;

        MiruRequest<CatwalkQuery> request = new MiruRequest<>("catwalkModelQueue",
            tenantId,
            MiruActorId.NOT_PROVIDED,
            MiruAuthzExpression.NOT_PROVIDED,
            updateModelRequest.catwalkQuery,
            MiruSolutionLogLevel.NONE);

        String endpoint = CatwalkConstants.CATWALK_PREFIX + CatwalkConstants.PARTITION_QUERY_ENDPOINT + "/" + updateModelRequest.partitionId;

        MiruResponse<CatwalkAnswer> catwalkResponse = tenantQueryRouting.query("", "catwalkModelQueue", readerClient, requestMapper, responseMapper,
            request, endpoint, CatwalkAnswer.class);

        if (catwalkResponse != null && catwalkResponse.answer != null) {
            ModelFeatureScores[] featureScores = new ModelFeatureScores[updateModelRequest.catwalkQuery.featureFields.length];
            for (int i = 0; i < featureScores.length; i++) {
                featureScores[i] = new ModelFeatureScores(catwalkResponse.answer.resultsClosed,
                    catwalkResponse.answer.results[i],
                    catwalkResponse.answer.timeRange);
            }
            return featureScores;
        } else {
            LOG.warn("Empty catwalk response from {}", updateModelRequest);
            return null;
        }
    }

    public void updateModel(MiruTenantId tenantId, String catwalkId, String modelId, int partitionId, CatwalkQuery catwalkQuery) throws Exception {
        long start = System.currentTimeMillis();

        byte[] modelKey = modelQueue.updateModelKey(tenantId, catwalkId, modelId, partitionId);

        EmbeddedClient processedClient = processedClient(tenantId, catwalkId, modelId);
        TimestampedValue timestampedValue = processedClient.getTimestampedValue(Consistency.quorum, null, modelKey);
        long lastProcessed = (timestampedValue != null) ? timestampedValue.getTimestampId() : -1;
        if ((System.currentTimeMillis() - lastProcessed) < modelUpdateIntervalInMillis) {
            return;
        }

        modelQueue.enqueue(tenantId, catwalkId, modelId, partitionId, catwalkQuery);

        EmbeddedClient catsClient = catsClient();
        catsClient.commit(Consistency.none,
            null,
            commitKeyValueStream -> commitKeyValueStream.commit(catwalkId.getBytes(StandardCharsets.UTF_8), new byte[0], -1, false),
            10_000,
            TimeUnit.MILLISECONDS);

        stats.ingressed("/miru/catwalk/model/" + tenantId.toString(), 1, System.currentTimeMillis() - start);
    }

    private EmbeddedClient catsClient() throws Exception {
        PartitionName partitionName = catsPartition();
        amzaService.getRingWriter().ensureMaximalRing(partitionName.getRingName());
        amzaService.setPropertiesIfAbsent(partitionName, CATS_WALKED);
        amzaService.awaitOnline(partitionName, 30_000);
        return embeddedClientProvider.getClient(partitionName);
    }

    private PartitionName catsPartition() {
        byte[] nameBytes = ("catwalkIds").getBytes(StandardCharsets.UTF_8);
        return new PartitionName(false, nameBytes, nameBytes);
    }

    private EmbeddedClient processedClient(MiruTenantId tenantId, String catwalkId, String modelId) throws Exception {
        int queueId = modelQueue.getQueueId(tenantId, catwalkId, modelId);
        return processedClient(queueId);
    }

    private EmbeddedClient processedClient(int queueId) throws Exception {
        PartitionName partitionName = processedPartition(queueId);
        amzaService.getRingWriter().ensureMaximalRing(partitionName.getRingName());
        amzaService.setPropertiesIfAbsent(partitionName, PROCESSED_MODEL);
        amzaService.awaitOnline(partitionName, 30_000);
        return embeddedClientProvider.getClient(partitionName);
    }

    private PartitionName processedPartition(int queueId) {
        byte[] nameBytes = ("processed-" + queueId).getBytes(StandardCharsets.UTF_8);
        return new PartitionName(false, nameBytes, nameBytes);
    }

}
