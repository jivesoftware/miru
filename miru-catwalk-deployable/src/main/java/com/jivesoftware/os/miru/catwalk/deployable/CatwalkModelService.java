package com.jivesoftware.os.miru.catwalk.deployable;

import com.google.common.collect.Iterators;
import com.google.common.collect.PeekingIterator;
import com.jivesoftware.os.amza.api.PartitionClient;
import com.jivesoftware.os.amza.api.PartitionClientProvider;
import com.jivesoftware.os.amza.api.filer.UIO;
import com.jivesoftware.os.amza.api.partition.Consistency;
import com.jivesoftware.os.amza.api.partition.Durability;
import com.jivesoftware.os.amza.api.partition.PartitionName;
import com.jivesoftware.os.amza.api.partition.PartitionProperties;
import com.jivesoftware.os.amza.api.stream.RowType;
import com.jivesoftware.os.amza.service.filer.HeapFiler;
import com.jivesoftware.os.miru.api.MiruStats;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.api.query.filter.MiruValue;
import com.jivesoftware.os.miru.stream.plugins.catwalk.CatwalkModel;
import com.jivesoftware.os.miru.stream.plugins.catwalk.FeatureScore;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

/**
 *
 */
public class CatwalkModelService {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    private static final PartitionProperties MODEL_PROPERTIES = new PartitionProperties(Durability.fsync_async,
        TimeUnit.DAYS.toMillis(30), TimeUnit.DAYS.toMillis(10), TimeUnit.DAYS.toMillis(30), TimeUnit.DAYS.toMillis(10), 0, 0, 0, 0,
        false, Consistency.leader_quorum, true, true, false, RowType.snappy_primary, "berkeleydb", null, -1, -1);

    private final ExecutorService readRepairers;
    private final PartitionClientProvider clientProvider;
    private final MiruStats stats;

    private final long additionalSolverAfterNMillis = 1_000; //TODO expose to conf?
    private final long abandonLeaderSolutionAfterNMillis = 5_000; //TODO expose to conf?
    private final long abandonSolutionAfterNMillis = 30_000; //TODO expose to conf?

    public CatwalkModelService(ExecutorService readRepairers, PartitionClientProvider clientProvider, MiruStats stats) {
        this.readRepairers = readRepairers;
        this.clientProvider = clientProvider;
        this.stats = stats;
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
    public CatwalkModel getModel(MiruTenantId tenantId, String userId, String[][] featureFieldIds) throws Exception {
        long start = System.currentTimeMillis();

        PartitionClient client = modelClient(tenantId);

        List<FeatureRange> deletableRanges = new ArrayList<>();
        Map<FieldIdsKey, MergedScores> fieldIdsToFeatureScores = new HashMap<>();
        FeatureRange[] currentRange = {null};
        long[] schema = {-1};
        client.scan(Consistency.leader_quorum,
            prefixedKeyRangeStream -> {
                for (int i = 0; i < featureFieldIds.length; i++) {
                    String[] fieldIds = featureFieldIds[i];
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
                    currentRange[0] = range;

                    ModelFeatureScores scores = valueFromBytes(value);
                    FieldIdsKey fieldIdsKey = new FieldIdsKey(range.fieldIds);
                    fieldIdsToFeatureScores.compute(fieldIdsKey, (t, currentMerged) -> {
                        if (currentMerged == null) {
                            return new MergedScores(range, scores);
                        }

                        PeekingIterator<FeatureScore> a = Iterators.peekingIterator(currentMerged.latestScores.featureScores.iterator());
                        PeekingIterator<FeatureScore> b = Iterators.peekingIterator(scores.featureScores.iterator());

                        List<FeatureScore> merged = new ArrayList<>(currentMerged.latestScores.featureScores.size() + scores.featureScores.size());
                        while (a.hasNext() || b.hasNext()) {

                            if (a.hasNext() && b.hasNext()) {
                                int c = FEATURE_SCORE_COMPARATOR.compare(a.peek(), b.peek());
                                if (c == 0) {
                                    merged.add(merge(a.next(), b.next()));
                                } else if (c < 0) {
                                    merged.add(a.next());
                                } else {
                                    merged.add(b.next());
                                }
                            } else if (a.hasNext()) {
                                merged.add(a.next());

                            } else if (b.hasNext()) {
                                merged.add(b.next());
                            }
                        }

                        currentMerged.closedPartition &= scores.partitionIsClosed;
                        if (currentMerged.closedPartition) {
                            if (currentMerged.ranges == null) {
                                currentMerged.ranges = new ArrayList<>();
                            }
                            currentMerged.ranges.add(currentMerged.latestRange);
                            currentMerged.scores = currentMerged.latestScores;
                        }

                        currentMerged.latestRange = range;
                        currentMerged.latestScores = new ModelFeatureScores(scores.partitionIsClosed, merged);
                        return currentMerged;
                    });

                } else {
                    deletableRanges.add(range);
                }
                return true;
            },
            additionalSolverAfterNMillis,
            abandonLeaderSolutionAfterNMillis,
            abandonSolutionAfterNMillis,
            Optional.<List<String>>empty());

        @SuppressWarnings("unchecked")
        List<FeatureScore>[] featureScores = new List[featureFieldIds.length];
        for (int i = 0; i < featureFieldIds.length; i++) {
            MergedScores mergedScores = fieldIdsToFeatureScores.get(new FieldIdsKey(featureFieldIds[i]));
            featureScores[i] = mergedScores.latestScores.featureScores;
        }

        for (Map.Entry<FieldIdsKey, MergedScores> entry : fieldIdsToFeatureScores.entrySet()) {
            List<FeatureRange> ranges = entry.getValue().ranges;
            if (ranges != null && ranges.size() > 1) {
                readRepairers.submit(new ReadRepair(tenantId, userId, entry.getKey(), entry.getValue()));
            }
        }

        if (!deletableRanges.isEmpty()) {
            readRepairers.submit(() -> {
                try {
                    removeModel(tenantId, userId, deletableRanges);
                } catch (Exception x) {
                    LOG.error("Failure while trying to delete.");
                }
            });
        }

        CatwalkModel model = new CatwalkModel(featureScores);
        stats.egressed("/miru/catwalk/model/" + tenantId.toString(), 1, System.currentTimeMillis() - start);
        return model;
    }

    public void saveModel(MiruTenantId tenantId,
        String userId,
        String[] fieldIds,
        int fromPartitionId,
        int toPartitionId,
        boolean partitionIsClosed,
        List<FeatureScore> scores) throws Exception {

        Collections.sort(scores, FEATURE_SCORE_COMPARATOR);

        PartitionClient client = modelClient(tenantId);
        byte[] key = userPartitionKey(userId, fieldIds, fromPartitionId, toPartitionId);
        byte[] value = valueToBytes(partitionIsClosed, scores);
        client.commit(Consistency.leader_quorum, null,
            commitKeyValueStream -> commitKeyValueStream.commit(key, value, -1, false),
            additionalSolverAfterNMillis,
            abandonSolutionAfterNMillis,
            Optional.<List<String>>empty());
    }

    public void removeModel(MiruTenantId tenantId,
        String userId,
        List<FeatureRange> ranges) throws Exception {

        PartitionClient client = modelClient(tenantId);
        client.commit(Consistency.leader_quorum, null,
            commitKeyValueStream -> {
                for (FeatureRange range : ranges) {
                    byte[] key = userPartitionKey(userId, range.fieldIds, range.fromPartitionId, range.toPartitionId);
                    if (!commitKeyValueStream.commit(key, null, -1, true)) {
                        return false;
                    }
                }

                return true;
            },
            additionalSolverAfterNMillis,
            abandonSolutionAfterNMillis,
            Optional.<List<String>>empty()
        );
    }

    private FeatureScore merge(FeatureScore a, FeatureScore b) {
        return new FeatureScore(a.values, a.numerator + b.numerator, a.denominator + b.denominator);
    }

    private byte[] valueToBytes(boolean partitionIsClosed, List<FeatureScore> scores) throws IOException {
        HeapFiler filer = new HeapFiler(1 + 1 + scores.size() * (8 + 8 + 4 + 4 + 10)); //TODO rough guesstimation

        UIO.writeByte(filer, (byte) 0, "version");
        UIO.writeByte(filer, partitionIsClosed ? (byte) 1 : (byte) 0, "partitionIsClosed");

        byte[] lengthBuffer = new byte[4];
        UIO.writeInt(filer, scores.size(), "scoresLength", lengthBuffer);
        for (FeatureScore score : scores) {
            UIO.writeInt(filer, score.values.length, "valuesLength", lengthBuffer);
            for (MiruValue value : score.values) {
                UIO.writeInt(filer, value.parts.length, "partsLength", lengthBuffer);
                for (String part : value.parts) {
                    UIO.writeByteArray(filer, part.getBytes(StandardCharsets.UTF_8), "part", lengthBuffer);
                }
            }
            UIO.writeLong(filer, score.numerator, "numerator");
            UIO.writeLong(filer, score.denominator, "denominator");
        }

        return filer.getBytes();
    }

    private ModelFeatureScores valueFromBytes(byte[] value) throws IOException {
        HeapFiler filer = HeapFiler.fromBytes(value, value.length);
        byte version = UIO.readByte(filer, "version");
        if (version != 0) {
            throw new IllegalStateException("Unexpected version " + version);
        }
        boolean partitionIsClosed = UIO.readByte(filer, "partitionIsClosed") == 1;
        byte[] lengthBuffer = new byte[8];
        List<FeatureScore> scores = new ArrayList<>(UIO.readInt(filer, "scoresLength", value));
        for (int i = 0; i < scores.size(); i++) {
            MiruValue[] values = new MiruValue[UIO.readInt(filer, "valuesLength", value)];
            for (int j = 0; j < values.length; j++) {
                String[] parts = new String[UIO.readInt(filer, "partsLength", value)];
                for (int k = 0; k < parts.length; k++) {
                    parts[k] = new String(UIO.readByteArray(filer, "part", lengthBuffer), StandardCharsets.UTF_8);
                }
                values[j] = new MiruValue(parts);
            }
            long numerator = UIO.readLong(filer, "numerator", lengthBuffer);
            long denominator = UIO.readLong(filer, "denominator", lengthBuffer);
            scores.add(new FeatureScore(values, numerator, denominator));
        }
        return new ModelFeatureScores(partitionIsClosed, scores);
    }

    private PartitionClient modelClient(MiruTenantId tenantId) throws Exception {
        byte[] nameBytes = ("model-" + tenantId).getBytes(StandardCharsets.UTF_8);
        return clientProvider.getPartition(new PartitionName(false, nameBytes, nameBytes), 3, MODEL_PROPERTIES);
    }

    private byte[] userPartitionKey(String userId, String[] fieldIds, int fromPartitionId, int toPartitionId) {
        byte[] userBytes = userId.getBytes(StandardCharsets.UTF_8);

        int fieldIdsSizeInByte = 0;
        byte[][] rawFields = new byte[fieldIds.length][];
        for (int i = 0; i < rawFields.length; i++) {
            fieldIdsSizeInByte += 4;
            rawFields[i] = fieldIds[i].getBytes(StandardCharsets.UTF_8);
            fieldIdsSizeInByte += rawFields[i].length;
        }

        int keyLength = 2 + userBytes.length + 1 + (fieldIdsSizeInByte) + 4 + 4;
        byte[] keyBytes = new byte[keyLength];
        int offset = 0;

        UIO.shortBytes((short) userBytes.length, keyBytes, 0);
        offset += 2;

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

        int fieldIdsLength = key[offset];
        offset++;

        String[] fieldIds = new String[fieldIdsLength];
        for (int i = 0; i < fieldIdsLength; i++) {
            int length = UIO.bytesInt(key, offset);
            offset += 4;
            byte[] rawField = new byte[length];
            UIO.readBytes(key, offset, rawField);
            offset += length;
            fieldIds[i] = new String(rawField, StandardCharsets.UTF_8);
        }

        int fromPartitionId = UIO.bytesInt(key, offset);
        offset += 4;

        int toPartitionId = Integer.MAX_VALUE - UIO.bytesInt(key, offset);
        offset += 4;

        return new FeatureRange(fieldIds, fromPartitionId, toPartitionId);
    }

    private static class FeatureRange {

        public final String[] fieldIds;
        public final int fromPartitionId;
        public final int toPartitionId;

        public FeatureRange(String[] fieldIds, int fromPartitionId, int toPartitionId) {
            this.fieldIds = fieldIds;
            this.fromPartitionId = fromPartitionId;
            this.toPartitionId = toPartitionId;
        }

        public boolean intersects(FeatureRange range) {
            if (!Arrays.equals(fieldIds, range.fieldIds)) {
                return false;
            }
            return fromPartitionId <= range.toPartitionId && range.fromPartitionId <= toPartitionId;
        }

        private FeatureRange merge(FeatureRange range) {
            if (!Arrays.equals(fieldIds, range.fieldIds)) {
                throw new IllegalStateException("Trying to merge ranges that are for a different set of fieldIds "
                    + Arrays.toString(fieldIds) + " vs " + Arrays.toString(range.fieldIds));
            }
            return new FeatureRange(fieldIds, Math.min(fromPartitionId, range.fromPartitionId), Math.max(toPartitionId, range.toPartitionId));
        }
    }

    private class ReadRepair implements Runnable {

        private final MiruTenantId tenantId;
        private final String userId;
        private final FieldIdsKey fieldIdsKey;
        private final MergedScores mergedScores;

        public ReadRepair(MiruTenantId tenantId, String userId, FieldIdsKey fieldIdsKey, MergedScores mergedScores) {
            this.tenantId = tenantId;
            this.userId = userId;
            this.fieldIdsKey = fieldIdsKey;
            this.mergedScores = mergedScores;
        }

        @Override
        public void run() {
            try {
                FeatureRange merged = null;
                for (FeatureRange range : mergedScores.ranges) {
                    if (merged == null) {
                        merged = range;
                    } else {
                        merged = merged.merge(range);
                    }
                }

                saveModel(tenantId, userId, fieldIdsKey.fieldIds, merged.fromPartitionId, merged.toPartitionId, true, mergedScores.scores.featureScores);
                removeModel(tenantId, userId, mergedScores.ranges);
            } catch (Exception x) {
                LOG.error("Failure while trying to apply read repairs.");
            }
        }

    }


    private static final FeatureScoreComparator FEATURE_SCORE_COMPARATOR = new FeatureScoreComparator();

    private static class FeatureScoreComparator implements Comparator<FeatureScore> {

        @Override
        public int compare(FeatureScore o1, FeatureScore o2) {
            int c = Integer.compare(o1.values.length, o2.values.length);
            if (c != 0) {
                return c;
            }
            for (int i = 0; i < o1.values.length; i++) {
                c = Integer.compare(o1.values[i].parts.length, o2.values[i].parts.length);
                if (c != 0) {
                    return c;
                }
                for (int j = 0; j < o1.values[i].parts.length; j++) {
                    c = o1.values[i].parts[j].compareTo(o2.values[i].parts[j]);
                    if (c != 0) {
                        return c;
                    }
                }
            }
            return c;
        }

    }

    private static class FieldIdsKey {

        private final String[] fieldIds;

        public FieldIdsKey(String[] fieldIds) {
            this.fieldIds = fieldIds;
        }

        @Override
        public int hashCode() {
            int hash = 3;
            hash = 23 * hash + Arrays.deepHashCode(this.fieldIds);
            return hash;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (obj == null) {
                return false;
            }
            if (getClass() != obj.getClass()) {
                return false;
            }
            final FieldIdsKey other = (FieldIdsKey) obj;
            if (!Arrays.deepEquals(this.fieldIds, other.fieldIds)) {
                return false;
            }
            return true;
        }

    }

    private static class MergedScores {

        boolean closedPartition = true;

        FeatureRange latestRange;
        ModelFeatureScores latestScores;

        List<FeatureRange> ranges;
        ModelFeatureScores scores;

        public MergedScores(FeatureRange latestRange, ModelFeatureScores latestScores) {
            this.latestRange = latestRange;
            this.latestScores = latestScores;
        }

    }

}
