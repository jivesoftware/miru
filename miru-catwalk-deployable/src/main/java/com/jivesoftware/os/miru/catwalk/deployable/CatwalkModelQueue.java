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

import com.jivesoftware.os.amza.api.filer.UIO;
import com.jivesoftware.os.amza.api.partition.Consistency;
import com.jivesoftware.os.amza.api.partition.Durability;
import com.jivesoftware.os.amza.api.partition.PartitionName;
import com.jivesoftware.os.amza.api.partition.PartitionProperties;
import com.jivesoftware.os.amza.api.ring.RingMember;
import com.jivesoftware.os.amza.api.stream.RowType;
import com.jivesoftware.os.amza.service.AmzaService;
import com.jivesoftware.os.amza.service.EmbeddedClientProvider;
import com.jivesoftware.os.amza.service.EmbeddedClientProvider.EmbeddedClient;
import com.jivesoftware.os.amza.service.filer.HeapFiler;
import com.jivesoftware.os.miru.api.MiruStats;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.plugin.solution.MiruResponse;
import com.jivesoftware.os.miru.stream.plugins.catwalk.CatwalkAnswer;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import com.jivesoftware.os.routing.bird.http.client.TenantAwareHttpClient;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 *
 * @author jonathan.colt
 */
public class CatwalkModelQueue {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    private static final PartitionProperties UPDATE_MODEL_QUEUE = new PartitionProperties(Durability.fsync_async,
        TimeUnit.DAYS.toMillis(30), TimeUnit.DAYS.toMillis(10), TimeUnit.DAYS.toMillis(30), TimeUnit.DAYS.toMillis(10), 0, 0, 0, 0,
        false, Consistency.leader_quorum, true, true, false, RowType.snappy_primary, "berkeleydb", null, -1, -1);

    private final CatwalkModelService modelService;
    private final ScheduledExecutorService queueConsumers;
    private final TenantAwareHttpClient<String> readerClient;
    private final ExecutorService modelUpdaters;
    private final AmzaService amzaService;
    private final EmbeddedClientProvider embeddedClientProvider;
    private final MiruStats stats;
    private final int numQueues;
    private final long checkQueuesForWorkEveryNMillis;

    public CatwalkModelQueue(CatwalkModelService modelService,
        ScheduledExecutorService queueConsumers,
        TenantAwareHttpClient<String> readerClient,
        ExecutorService modelUpdaters,
        AmzaService amzaService,
        EmbeddedClientProvider embeddedClientProvider,
        MiruStats stats,
        int numQueues,
        long checkQueuesForWorkEvenNMillis) {

        this.modelService = modelService;
        this.queueConsumers = queueConsumers;
        this.readerClient = readerClient;
        this.modelUpdaters = modelUpdaters;
        this.amzaService = amzaService;
        this.embeddedClientProvider = embeddedClientProvider;
        this.stats = stats;
        this.numQueues = numQueues;
        this.checkQueuesForWorkEveryNMillis = checkQueuesForWorkEvenNMillis;
    }

    public void start() throws Exception {
        for (int q = 0; q < numQueues; q++) {
            queueConsumers.scheduleWithFixedDelay(new ScheduledQueueConsumer(q), 0, checkQueuesForWorkEveryNMillis, TimeUnit.MILLISECONDS);
        }
    }

    private final class ScheduledQueueConsumer implements Runnable {

        private final int queueId;

        public ScheduledQueueConsumer(int queueId) {
            this.queueId = queueId;
        }

        @Override
        public void run() {
            try {
                if (isLeader(queueId)) {
                    EmbeddedClient queueClient = queueClient(queueId);
                    List<UpdateModelRequest> batch = new ArrayList<>(1024); // TODO config
                    queueClient.scan(null, (byte[] prefix, byte[] key, byte[] value, long timestamp, long version) -> {
                        UpdateModelRequest request = updateModelRequestFromBytes(key);
                        batch.add(request);
                        return true;
                    });

                    if (batch.isEmpty()) {
                        return;
                    }

                    for (UpdateModelRequest request : batch) {
                        if (isLeader(queueId)) {
                            ModelFeatureScores model = fetchModel(request);
                            modelService.saveModel(request.tenantId,
                                request.userId,
                                request.fieldIds,
                                request.partitionId,
                                request.partitionId,
                                model.partitionIsClosed,
                                model.featureScores);

                            byte[] key = updateModelKey(request.tenantId, request.userId, request.fieldIds, request.partitionId);
                            queueClient.commit(Consistency.leader_quorum,
                                null, (commitKeyValueStream) -> commitKeyValueStream.commit(key, null, -1, true),
                                10_000, // TODO config
                                TimeUnit.MILLISECONDS);
                        } else {
                            return;
                        }
                    }
                }
            } catch (Exception x) {
                LOG.error("Unexpect issue while cheching queue:" + queueId, x);
            }
        }
    }

    private ModelFeatureScores fetchModel(UpdateModelRequest updateModelRequest) {
        MiruResponse<CatwalkAnswer> catwalkResponse = null;
        if (catwalkResponse != null && catwalkResponse.answer != null) {
            boolean partitionIsClosed = false; // TODO
            return new ModelFeatureScores(partitionIsClosed, catwalkResponse.answer.results[0]);
        } else {
            LOG.warn("Empty catwalk response from {}", updateModelRequest);
            return null;
        }
    }

    private boolean isLeader(int queueId) throws Exception {
        RingMember leader = amzaService.awaitLeader(queuePartition(queueId), 0);
        return leader != null && leader.equals(amzaService.getRingReader().getRingMember());
    }

    public void updateModel(MiruTenantId tenantId, String userId, String[][] featureFields, int partitionId) throws Exception {
        long start = System.currentTimeMillis();

        EmbeddedClient client = queueClient(tenantId, userId);

        client.commit(Consistency.leader_quorum,
            null,
            commitKeyValueStream -> {
                for (int i = 0; i < featureFields.length; i++) {
                    byte[] key = updateModelKey(tenantId, userId, featureFields[i], partitionId);
                    if (!commitKeyValueStream.commit(key, new byte[0], -1, false)) {
                        return false;
                    }
                }
                return true;
            },
            10_000,
            TimeUnit.MILLISECONDS);
        stats.ingressed("/miru/catwalk/model/" + tenantId.toString(), 1, System.currentTimeMillis() - start);

    }

    private EmbeddedClient queueClient(MiruTenantId tenantId, String userId) throws Exception {
        int queueId = Math.abs((tenantId.hashCode() + userId.hashCode()) % numQueues);
        return queueClient(queueId);
    }

    private EmbeddedClient queueClient(int queueId) throws Exception {
        PartitionName partitionName = queuePartition(queueId);
        amzaService.getRingWriter().ensureMaximalRing(partitionName.getRingName());
        amzaService.setPropertiesIfAbsent(partitionName, UPDATE_MODEL_QUEUE);
        return embeddedClientProvider.getClient(partitionName);
    }

    private PartitionName queuePartition(int queueId) {
        byte[] nameBytes = ("queue-" + queueId).getBytes(StandardCharsets.UTF_8);
        PartitionName partitionName = new PartitionName(false, nameBytes, nameBytes);
        return partitionName;
    }

    private byte[] updateModelKey(MiruTenantId tenantId, String userId, String[] fieldIds, int fromPartitionId) {
        byte[] tenantBytes = tenantId.getBytes();
        byte[] userBytes = userId.getBytes(StandardCharsets.UTF_8);

        int fieldIdsSizeInByte = 0;
        byte[][] rawFields = new byte[fieldIds.length][];
        for (int i = 0; i < rawFields.length; i++) {
            fieldIdsSizeInByte += 4;
            rawFields[i] = fieldIds[i].getBytes(StandardCharsets.UTF_8);
            fieldIdsSizeInByte += rawFields[i].length;
        }

        int keyLength = 4 + tenantBytes.length + 4 + userBytes.length + 1 + (fieldIdsSizeInByte) + 4;
        byte[] keyBytes = new byte[keyLength];
        int offset = 0;

        UIO.intBytes(tenantBytes.length, keyBytes, 0);
        offset += 4;

        UIO.writeBytes(tenantBytes, keyBytes, offset);
        offset += tenantBytes.length;

        UIO.intBytes(userBytes.length, keyBytes, 0);
        offset += 4;

        UIO.writeBytes(userBytes, keyBytes, offset);
        offset += userBytes.length;

        keyBytes[offset] = (byte) fieldIds.length;
        offset++;

        for (int i = 0; i < fieldIds.length; i++) {
            UIO.intBytes(rawFields[i].length, keyBytes, offset);
            offset += 4;
            offset += UIO.writeBytes(rawFields[i], keyBytes, offset);
        }

        UIO.intBytes(fromPartitionId, keyBytes, offset);
        offset += 4;

        return keyBytes;
    }

    private UpdateModelRequest updateModelRequestFromBytes(byte[] bytes) throws IOException {
        byte[] buffer = new byte[8];
        HeapFiler filer = HeapFiler.fromBytes(bytes, bytes.length);
        byte[] rawTenant = UIO.readByteArray(filer, "tenant", buffer);
        byte[] rawUser = UIO.readByteArray(filer, "user", buffer);
        int fieldCount = UIO.readByte(filer, "fieldCount");
        String[] fieldIds = new String[fieldCount];
        for (int i = 0; i < fieldCount; i++) {
            fieldIds[i] = new String(UIO.readByteArray(filer, "fieldId", buffer), StandardCharsets.UTF_8);
        }
        int partitionId = UIO.readInt(filer, "partitionId", buffer);
        return new UpdateModelRequest(new MiruTenantId(rawTenant), new String(rawUser, StandardCharsets.UTF_8), fieldIds, partitionId);
    }

    static class UpdateModelRequest {

        private final MiruTenantId tenantId;
        private final String userId;
        private final String[] fieldIds;
        private final int partitionId;

        public UpdateModelRequest(MiruTenantId tenantId, String userId, String[] fieldIds, int partitionId) {
            this.tenantId = tenantId;
            this.userId = userId;
            this.fieldIds = fieldIds;
            this.partitionId = partitionId;
        }

    }

}
