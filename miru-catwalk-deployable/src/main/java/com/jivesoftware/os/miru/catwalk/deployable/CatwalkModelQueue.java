package com.jivesoftware.os.miru.catwalk.deployable;

import com.fasterxml.jackson.databind.ObjectMapper;
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
import com.jivesoftware.os.amza.service.Partition.ScanRange;
import com.jivesoftware.os.amza.service.filer.HeapFiler;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.stream.plugins.catwalk.CatwalkQuery;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 *
 */
public class CatwalkModelQueue {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    private static final PartitionProperties UPDATE_MODEL_QUEUE = new PartitionProperties(Durability.fsync_async,
        TimeUnit.DAYS.toMillis(30), TimeUnit.DAYS.toMillis(10), TimeUnit.DAYS.toMillis(30), TimeUnit.DAYS.toMillis(10), 0, 0, 0, 0,
        false, Consistency.quorum, true, true, false, RowType.snappy_primary, "lab", -1, null, -1, -1);

    private final AmzaService amzaService;
    private final EmbeddedClientProvider embeddedClientProvider;
    private final ObjectMapper requestMapper;
    private final int numQueues;

    public CatwalkModelQueue(AmzaService amzaService,
        EmbeddedClientProvider embeddedClientProvider,
        ObjectMapper requestMapper,
        int numQueues) {
        this.amzaService = amzaService;
        this.embeddedClientProvider = embeddedClientProvider;
        this.requestMapper = requestMapper;
        this.numQueues = numQueues;
    }

    public int getQueueId(MiruTenantId tenantId, String catwalkId, String modelId) {
        int hash = tenantCatwalkModelHash(tenantId, catwalkId, modelId);
        return Math.abs(hash % numQueues);
    }

    private int tenantCatwalkModelHash(MiruTenantId tenantId, String catwalkId, String modelId) {
        return 31 * (31 * tenantId.hashCode() + catwalkId.hashCode()) + modelId.hashCode();
    }

    public boolean isLeader(int queueId) throws Exception {
        RingMember leader = amzaService.awaitLeader(queuePartition(queueId), 60_000);
        return leader != null && leader.equals(amzaService.getRingReader().getRingMember());
    }

    public List<UpdateModelRequest> getBatch(int queueId, int batchSize) throws Exception {
        EmbeddedClient queueClient = queueClient(queueId);
        List<UpdateModelRequest> batch = new ArrayList<>(batchSize);
        queueClient.scan(
            Collections.singletonList(ScanRange.ROW_SCAN),
            (prefix, key, value, timestamp, version) -> {
                if (timestamp <= System.currentTimeMillis()) {
                    UpdateModelRequest request = updateModelRequestFromBytes(requestMapper, key, value, timestamp);
                    batch.add(request);
                }
                return (batch.size() < batchSize);
            },
            true
        );
        return batch;
    }

    static byte[] updateModelKey(MiruTenantId tenantId, String catwalkId, String modelId, int partitionId) {
        byte[] tenantBytes = tenantId.getBytes();
        byte[] catwalkBytes = catwalkId.getBytes(StandardCharsets.UTF_8);
        byte[] modelBytes = modelId.getBytes(StandardCharsets.UTF_8);

        int keyLength = 4 + tenantBytes.length + 4 + catwalkBytes.length + 4 + modelBytes.length + 4;
        byte[] keyBytes = new byte[keyLength];
        int offset = 0;

        UIO.intBytes(tenantBytes.length, keyBytes, offset);
        offset += 4;

        UIO.writeBytes(tenantBytes, keyBytes, offset);
        offset += tenantBytes.length;

        UIO.intBytes(catwalkBytes.length, keyBytes, offset);
        offset += 4;

        UIO.writeBytes(catwalkBytes, keyBytes, offset);
        offset += catwalkBytes.length;

        UIO.intBytes(modelBytes.length, keyBytes, offset);
        offset += 4;

        UIO.writeBytes(modelBytes, keyBytes, offset);
        offset += modelBytes.length;

        UIO.intBytes(partitionId, keyBytes, offset);
        offset += 4;

        return keyBytes;
    }

    static UpdateModelRequest updateModelRequestFromBytes(ObjectMapper mapper, byte[] keyBytes, byte[] valueBytes, long timestamp) throws IOException {
        byte[] buffer = new byte[8];
        HeapFiler filer = HeapFiler.fromBytes(keyBytes, keyBytes.length);
        byte[] rawTenant = UIO.readByteArray(filer, "tenant", buffer);
        byte[] rawCatwalkId = UIO.readByteArray(filer, "catwalkId", buffer);
        byte[] rawModelId = UIO.readByteArray(filer, "modelId", buffer);
        int partitionId = UIO.readInt(filer, "partitionId", buffer);

        CatwalkQuery catwalkQuery = mapper.readValue(valueBytes, CatwalkQuery.class);

        return new UpdateModelRequest(new MiruTenantId(rawTenant),
            new String(rawCatwalkId, StandardCharsets.UTF_8),
            new String(rawModelId, StandardCharsets.UTF_8),
            partitionId,
            catwalkQuery,
            timestamp);
    }

    private PartitionName queuePartition(int queueId) {
        byte[] nameBytes = ("queue-" + queueId).getBytes(StandardCharsets.UTF_8);
        return new PartitionName(false, nameBytes, nameBytes);
    }

    private EmbeddedClient queueClient(MiruTenantId tenantId, String catwalkId, String modelId) throws Exception {
        int hash = tenantCatwalkModelHash(tenantId, catwalkId, modelId);
        int queueId = Math.abs(hash % numQueues);
        return queueClient(queueId);
    }

    private EmbeddedClient queueClient(int queueId) throws Exception {
        PartitionName partitionName = queuePartition(queueId);
        amzaService.getRingWriter().ensureMaximalRing(partitionName.getRingName(), 30_000L); //TODO config
        amzaService.createPartitionIfAbsent(partitionName, UPDATE_MODEL_QUEUE);
        amzaService.awaitOnline(partitionName, 30_000L); //TODO config
        return embeddedClientProvider.getClient(partitionName);
    }

    public void enqueue(MiruTenantId tenantId, String catwalkId, String modelId, int partitionId, CatwalkQuery catwalkQuery) throws Exception {

        byte[] modelKey = updateModelKey(tenantId, catwalkId, modelId, partitionId);
        byte[] modelValue = requestMapper.writeValueAsBytes(catwalkQuery);

        EmbeddedClient queueClient = queueClient(tenantId, catwalkId, modelId);
        queueClient.commit(Consistency.quorum,
            null,
            commitKeyValueStream -> commitKeyValueStream.commit(modelKey, modelValue, -1, false),
            10_000,
            TimeUnit.MILLISECONDS);
    }

    public void handleProcessed(int queueId, List<UpdateModelRequest> processedRequests, long delayInMillis) throws Exception {
        EmbeddedClient queueClient = queueClient(queueId);
        queueClient.commit(Consistency.quorum,
            null,
            (commitKeyValueStream) -> {
                for (UpdateModelRequest request : processedRequests) {
                    if (request.removeFromQueue) {
                        byte[] key = updateModelKey(request.tenantId, request.catwalkId, request.modelId, request.partitionId);
                        if (!commitKeyValueStream.commit(key, null, request.timestamp, true)) {
                            return false;
                        }
                    } else if (request.delayInQueue) {
                        byte[] key = updateModelKey(request.tenantId, request.catwalkId, request.modelId, request.partitionId);
                        byte[] modelValue = requestMapper.writeValueAsBytes(request.catwalkQuery);
                        if (!commitKeyValueStream.commit(key, modelValue, System.currentTimeMillis() + delayInMillis, false)) {
                            return false;
                        }
                    }
                }
                return true;
            },
            10_000, // TODO config
            TimeUnit.MILLISECONDS);
    }

}
