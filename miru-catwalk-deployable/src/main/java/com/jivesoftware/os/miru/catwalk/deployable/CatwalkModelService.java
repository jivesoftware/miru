package com.jivesoftware.os.miru.catwalk.deployable;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.jivesoftware.os.amza.api.PartitionClient;
import com.jivesoftware.os.amza.api.PartitionClientProvider;
import com.jivesoftware.os.amza.api.filer.UIO;
import com.jivesoftware.os.amza.api.partition.Consistency;
import com.jivesoftware.os.amza.api.partition.Durability;
import com.jivesoftware.os.amza.api.partition.PartitionName;
import com.jivesoftware.os.amza.api.partition.PartitionProperties;
import com.jivesoftware.os.amza.api.stream.RowType;
import com.jivesoftware.os.amza.client.http.AmzaClientProvider;
import com.jivesoftware.os.amza.service.filer.HeapFiler;
import com.jivesoftware.os.miru.api.MiruStats;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.api.query.filter.MiruValue;
import com.jivesoftware.os.miru.stream.plugins.catwalk.CatwalkModel;
import com.jivesoftware.os.miru.stream.plugins.catwalk.FeatureScore;
import com.jivesoftware.os.routing.bird.http.client.HttpClient;
import com.jivesoftware.os.routing.bird.http.client.HttpClientException;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

/**
 *
 */
public class CatwalkModelService {

    private final PartitionClientProvider clientProvider;
    private final MiruStats stats;
    private final ObjectMapper mapper;

    private static final PartitionProperties MODEL_PROPERTIES = new PartitionProperties(Durability.fsync_async,
        TimeUnit.DAYS.toMillis(30), TimeUnit.DAYS.toMillis(10), TimeUnit.DAYS.toMillis(30), TimeUnit.DAYS.toMillis(10), 0, 0, 0, 0,
        false, Consistency.leader_quorum, true, true, false, RowType.snappy_primary, "berkeleydb", null, -1, -1);

    private final long additionalSolverAfterNMillis = 1_000; //TODO expose to conf?
    private final long abandonLeaderSolutionAfterNMillis = 5_000; //TODO expose to conf?
    private final long abandonSolutionAfterNMillis = 30_000; //TODO expose to conf?

    public CatwalkModelService(PartitionClientProvider clientProvider, MiruStats stats, ObjectMapper mapper) {
        this.clientProvider = clientProvider;
        this.stats = stats;
        this.mapper = mapper;
    }

    // 1 - 9
    // 1 - 2
    // 3 - 7
    // 3 - 5
    // 6 - 7
    // 8 - 8
    // 9 - 9
    // 10 - 10
    // 11 - 11

    public CatwalkModel getModel(MiruTenantId tenantId, String userId, int[][] featureFieldIds) throws Exception {
        long start = System.currentTimeMillis();

        PartitionClient client = modelClient(tenantId);

        @SuppressWarnings("unchecked")
        List<FeatureScore>[] featureScores = new List[featureFieldIds.length];

        int[][] currentFieldIds = { null };
        FeatureRange[] currentRange = { null };
        client.scan(Consistency.leader_quorum,
            prefixedKeyRangeStream -> {
                for (int i = 0; i < featureFieldIds.length; i++) {
                    int[] fieldIds = featureFieldIds[i];
                    byte[] fromKey = userPartitionKey(userId, fieldIds, 0, 0);
                    byte[] toKey = userPartitionKey(userId, fieldIds, 0, Integer.MAX_VALUE);
                    if (!prefixedKeyRangeStream.stream(null, fromKey, null, toKey)) {
                        return false;
                    }
                }
                return true;
            },
            (prefix, key, value, timestamp, version) -> {

                FeatureRange range = getFeatureRange(key);
                if (currentRange[0] == null || !currentRange[0].intersects(range)) {
                    // handle it!
                    currentRange[0] = range;
                }
                return true;
            },
            additionalSolverAfterNMillis,
            abandonLeaderSolutionAfterNMillis,
            abandonSolutionAfterNMillis,
            Optional.<List<String>>empty());

        CatwalkModel model = new CatwalkModel(featureScores);

        stats.egressed("/miru/catwalk/model/" + tenantId.toString(), 1, System.currentTimeMillis() - start);
        return model;
    }

    public void updateModel(MiruTenantId tenantId, String userId, int partitionId) {
        long start = System.currentTimeMillis();
        stats.ingressed("/miru/catwalk/model/" + tenantId.toString(), 1, System.currentTimeMillis() - start);
    }

    public void saveModel(MiruTenantId tenantId,
        String userId,
        int[] fieldIds,
        int fromPartitionId,
        int toPartitionId,
        List<FeatureScore> scores) throws Exception {

        PartitionClient client = modelClient(tenantId);
        byte[] key = userPartitionKey(userId, fieldIds, fromPartitionId, toPartitionId);
        byte[] value = valueToBytes(scores);
        client.commit(Consistency.leader_quorum, null,
            commitKeyValueStream -> commitKeyValueStream.commit(key, value, -1, false),
            additionalSolverAfterNMillis,
            abandonSolutionAfterNMillis,
            Optional.<List<String>>empty());
    }

    private byte[] valueToBytes(List<FeatureScore> scores) throws IOException {
        HeapFiler filer = new HeapFiler(scores.size() * (8 + 8 + 4 + 4 + 10)); //TODO rough guesstimation

        byte[] lengthBuffer = new byte[4];
        UIO.writeInt(filer, scores.size(), "scores", lengthBuffer);
        for (FeatureScore score : scores) {
            UIO.writeInt(filer, score.values.length, "values", lengthBuffer);
            for (MiruValue value : score.values) {
                UIO.writeInt(filer, value.parts.length, "parts", lengthBuffer);
                for (String part : value.parts) {
                    UIO.writeByteArray(filer, part.getBytes(StandardCharsets.UTF_8), "part", lengthBuffer);
                }
            }
            UIO.writeLong(filer, score.numerator, "numerator");
            UIO.writeLong(filer, score.denominator, "denominator");
        }

        return filer.getBytes();
    }

    private PartitionClient modelClient(MiruTenantId tenantId) throws Exception {
        byte[] nameBytes = ("model-" + tenantId).getBytes(StandardCharsets.UTF_8);
        return clientProvider.getPartition(new PartitionName(false, nameBytes, nameBytes), 3, MODEL_PROPERTIES);
    }

    private byte[] userPartitionKey(String userId, int[] fieldIds, int fromPartitionId, int toPartitionId) {
        byte[] userBytes = userId.getBytes(StandardCharsets.UTF_8);

        int keyLength = 2 + userBytes.length + 1 + (fieldIds.length * 2) + 4 + 4;
        byte[] keyBytes = new byte[keyLength];
        int offset = 0;

        UIO.shortBytes((short) userBytes.length, userBytes, 0);
        offset += 2;

        UIO.writeBytes(userBytes, keyBytes, offset);
        offset += userBytes.length;

        keyBytes[offset] = (byte) fieldIds.length;
        offset++;

        for (int i = 0; i < fieldIds.length; i++) {
            UIO.shortBytes((short) fieldIds[i], keyBytes, offset);
            offset += 2;
        }

        UIO.intBytes(fromPartitionId, keyBytes, offset);
        offset += 4;

        // flip so highest toPartitionId sorts first (relative to fromPartitionId)
        UIO.intBytes(Integer.MAX_VALUE - toPartitionId, keyBytes, offset);
        offset += 4;

        return keyBytes;
    }

    private FeatureRange getFeatureRange(byte[] key) {

        int offset = 0;
        int userLength = UIO.bytesShort(key, offset);
        offset += 2;
        offset += userLength;

        int fieldIdsLength = (int) key[offset];
        offset++;

        int[] fieldIds = new int[fieldIdsLength];
        for (int i = 0; i < fieldIdsLength; i++) {
            fieldIds[i] = (int) UIO.bytesShort(key, offset);
            offset += 2;
        }

        int fromPartitionId = UIO.bytesInt(key, offset);
        offset += 4;

        int toPartitionId = Integer.MAX_VALUE - UIO.bytesInt(key, offset);
        offset += 4;

        return new FeatureRange(fieldIds, fromPartitionId, toPartitionId);
    }

    public static class FeatureRange {

        public final int[] fieldIds;
        public final int fromPartitionId;
        public final int toPartitionId;

        public FeatureRange(int[] fieldIds, int fromPartitionId, int toPartitionId) {
            this.fieldIds = fieldIds;
            this.fromPartitionId = fromPartitionId;
            this.toPartitionId = toPartitionId;
        }

        public boolean intersects(FeatureRange range) {
            return fromPartitionId <= range.toPartitionId && range.fromPartitionId <= toPartitionId;
        }
    }
}
