package com.jivesoftware.os.miru.catwalk.deployable;

import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.PeekingIterator;
import com.google.common.collect.Sets;
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
import com.jivesoftware.os.miru.api.base.MiruTermId;
import com.jivesoftware.os.miru.plugin.solution.MiruTimeRange;
import com.jivesoftware.os.miru.stream.plugins.catwalk.CatwalkModel;
import com.jivesoftware.os.miru.stream.plugins.catwalk.CatwalkQuery;
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
import java.util.TreeSet;
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

    private final CatwalkModelQueue modelQueue;
    private final ExecutorService readRepairers;
    private final PartitionClientProvider clientProvider;
    private final MiruStats stats;

    private final long additionalSolverAfterNMillis = 1_000; //TODO expose to conf?
    private final long abandonLeaderSolutionAfterNMillis = 5_000; //TODO expose to conf?
    private final long abandonSolutionAfterNMillis = 30_000; //TODO expose to conf?

    public CatwalkModelService(CatwalkModelQueue modelQueue,
        ExecutorService readRepairers,
        PartitionClientProvider clientProvider,
        MiruStats stats) {
        this.modelQueue = modelQueue;
        this.readRepairers = readRepairers;
        this.clientProvider = clientProvider;
        this.stats = stats;
    }

    public CatwalkModel getModel(MiruTenantId tenantId, String catwalkId, String modelId, CatwalkQuery catwalkQuery) throws Exception {
        long start = System.currentTimeMillis();

        PartitionClient client = modelClient(tenantId);

        List<FeatureRange> deletableRanges = Lists.newArrayList();
        TreeSet<Integer> partitionIds = Sets.newTreeSet();

        Map<FieldIdsKey, MergedScores> fieldIdsToFeatureScores = new HashMap<>();
        FeatureRange[] currentRange = {null};
        String[][] featureFields = catwalkQuery.featureFields;
        client.scan(Consistency.leader_quorum,
            prefixedKeyRangeStream -> {
                for (int i = 0; i < featureFields.length; i++) {
                    String[] fields = featureFields[i];
                    byte[] fromKey = modelPartitionKey(catwalkId, modelId, fields, 0, 0);
                    byte[] toKey = modelPartitionKey(catwalkId, modelId, fields, 0, Integer.MAX_VALUE);
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

                    for (int partitionId = range.fromPartitionId; partitionId <= range.toPartitionId; partitionId++) {
                        partitionIds.add(partitionId);
                    }

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

                        currentMerged.contiguousClosedPartitions &= scores.partitionIsClosed;
                        currentMerged.contiguousClosedPartitions &= (currentMerged.latestRange.toPartitionId + 1 == range.fromPartitionId);
                        if (currentMerged.contiguousClosedPartitions) {
                            if (currentMerged.ranges == null) {
                                currentMerged.ranges = new ArrayList<>();
                            }
                            currentMerged.ranges.add(currentMerged.latestRange);
                            currentMerged.scores = currentMerged.latestScores;
                            if (currentMerged.timeRange == null) {
                                currentMerged.timeRange = currentMerged.latestScores.timeRange;
                            } else {
                                currentMerged.timeRange = new MiruTimeRange(
                                    Math.min(scores.timeRange.smallestTimestamp, currentMerged.timeRange.smallestTimestamp),
                                    Math.max(scores.timeRange.largestTimestamp, currentMerged.timeRange.largestTimestamp));
                            }
                        }

                        currentMerged.latestRange = range;
                        currentMerged.latestScores = new ModelFeatureScores(scores.partitionIsClosed, merged, scores.timeRange);
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
        List<FeatureScore>[] featureScores = new List[featureFields.length];
        for (int i = 0; i < featureFields.length; i++) {
            MergedScores mergedScores = fieldIdsToFeatureScores.get(new FieldIdsKey(featureFields[i]));
            featureScores[i] = mergedScores.latestScores.featureScores;
        }

        for (Map.Entry<FieldIdsKey, MergedScores> entry : fieldIdsToFeatureScores.entrySet()) {
            List<FeatureRange> ranges = entry.getValue().ranges;
            if (ranges != null && ranges.size() > 1) {
                readRepairers.submit(new ReadRepair(tenantId, catwalkId, modelId, entry.getKey(), entry.getValue()));
            }
        }

        if (!deletableRanges.isEmpty()) {
            readRepairers.submit(() -> {
                try {
                    removeModel(tenantId, catwalkId, modelId, deletableRanges);
                } catch (Exception x) {
                    LOG.error("Failure while trying to delete.");
                }
            });
        }

        Integer[] repairPartitionIds = partitionIds.toArray(new Integer[0]);
        for (int i = 1; i < repairPartitionIds.length; i++) {
            int lastPartitionId = repairPartitionIds[i - 1];
            int currentPartitionId = repairPartitionIds[i];
            for (int partitionId = lastPartitionId + 1; partitionId < currentPartitionId; partitionId++) {
                modelQueue.enqueue(tenantId, catwalkId, modelId, partitionId, catwalkQuery);
            }
        }

        CatwalkModel model = new CatwalkModel(featureScores);
        stats.egressed("/miru/catwalk/model/" + tenantId.toString(), 1, System.currentTimeMillis() - start);
        return model;
    }

    public void saveModel(MiruTenantId tenantId,
        String catwalkId,
        String modelId,
        int fromPartitionId,
        int toPartitionId,
        String[][] featureFields,
        ModelFeatureScores[] models) throws Exception {

        for (ModelFeatureScores model : models) {
            Collections.sort(model.featureScores, FEATURE_SCORE_COMPARATOR);
        }

        PartitionClient client = modelClient(tenantId);
        client.commit(Consistency.leader_quorum, null,
            commitKeyValueStream -> {
                for (int i = 0; i < featureFields.length; i++) {
                    byte[] key = modelPartitionKey(catwalkId, modelId, featureFields[i], fromPartitionId, toPartitionId);
                    byte[] value = valueToBytes(models[i].partitionIsClosed, models[i].featureScores, models[i].timeRange);
                    if (!commitKeyValueStream.commit(key, value, -1, false)) {
                        return false;
                    }
                }
                return true;
            },
            additionalSolverAfterNMillis,
            abandonSolutionAfterNMillis,
            Optional.<List<String>>empty());
    }

    public void removeModel(MiruTenantId tenantId,
        String catwalkId,
        String modelId,
        List<FeatureRange> ranges) throws Exception {

        PartitionClient client = modelClient(tenantId);
        client.commit(Consistency.leader_quorum, null,
            commitKeyValueStream -> {
                for (FeatureRange range : ranges) {
                    byte[] key = modelPartitionKey(catwalkId, modelId, range.fieldIds, range.fromPartitionId, range.toPartitionId);
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
        return new FeatureScore(a.termIds, a.numerator + b.numerator, a.denominator + b.denominator);
    }

    private byte[] valueToBytes(boolean partitionIsClosed, List<FeatureScore> scores, MiruTimeRange timeRange) throws IOException {
        HeapFiler filer = new HeapFiler(1 + 1 + scores.size() * (8 + 8 + 4 + 4 + 10) + 8 + 8); //TODO rough guesstimation

        UIO.writeByte(filer, (byte) 0, "version");
        UIO.writeByte(filer, partitionIsClosed ? (byte) 1 : (byte) 0, "partitionIsClosed");

        byte[] lengthBuffer = new byte[4];
        UIO.writeInt(filer, scores.size(), "scoresLength", lengthBuffer);
        for (FeatureScore score : scores) {
            UIO.writeInt(filer, score.termIds.length, "termsLength", lengthBuffer);
            for (MiruTermId termId : score.termIds) {
                UIO.writeByteArray(filer, termId.getBytes(), "term", lengthBuffer);
            }
            UIO.writeLong(filer, score.numerator, "numerator");
            UIO.writeLong(filer, score.denominator, "denominator");
        }

        UIO.writeLong(filer, timeRange.smallestTimestamp, "smallestTimestamp");
        UIO.writeLong(filer, timeRange.largestTimestamp, "largestTimestamp");

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
            MiruTermId[] terms = new MiruTermId[UIO.readInt(filer, "termsLength", value)];
            for (int j = 0; j < terms.length; j++) {
                terms[j] = new MiruTermId(UIO.readByteArray(filer, "term", lengthBuffer));
            }
            long numerator = UIO.readLong(filer, "numerator", lengthBuffer);
            long denominator = UIO.readLong(filer, "denominator", lengthBuffer);
            scores.add(new FeatureScore(terms, numerator, denominator));
        }

        MiruTimeRange timeRange = new MiruTimeRange(
            UIO.readLong(filer, "smallestTimestamp", lengthBuffer),
            UIO.readLong(filer, "largestTimestamp", lengthBuffer));
        return new ModelFeatureScores(partitionIsClosed, scores, timeRange);
    }

    private PartitionClient modelClient(MiruTenantId tenantId) throws Exception {
        byte[] nameBytes = ("model-" + tenantId).getBytes(StandardCharsets.UTF_8);
        return clientProvider.getPartition(new PartitionName(false, nameBytes, nameBytes), 3, MODEL_PROPERTIES);
    }

    private byte[] modelPartitionKey(String catwalkId, String modelId, String[] fields, int fromPartitionId, int toPartitionId) {
        byte[] catwalkBytes = catwalkId.getBytes(StandardCharsets.UTF_8);
        byte[] modelBytes = modelId.getBytes(StandardCharsets.UTF_8);

        int fieldsSizeInByte = 0;
        byte[][] rawFields = new byte[fields.length][];
        for (int i = 0; i < rawFields.length; i++) {
            fieldsSizeInByte += 4;
            rawFields[i] = fields[i].getBytes(StandardCharsets.UTF_8);
            fieldsSizeInByte += rawFields[i].length;
        }

        int keyLength = 2 + catwalkBytes.length + 2 + modelBytes.length + 1 + (fieldsSizeInByte) + 4 + 4;
        byte[] keyBytes = new byte[keyLength];
        int offset = 0;

        UIO.shortBytes((short) catwalkBytes.length, keyBytes, 0);
        offset += 2;

        UIO.writeBytes(catwalkBytes, keyBytes, offset);
        offset += catwalkBytes.length;

        UIO.shortBytes((short) modelBytes.length, keyBytes, 0);
        offset += 2;

        UIO.writeBytes(modelBytes, keyBytes, offset);
        offset += modelBytes.length;

        keyBytes[offset] = (byte) fields.length;
        offset++;

        for (int i = 0; i < fields.length; i++) {
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
        int modelLength = UIO.bytesShort(key, offset);
        offset += 2;
        offset += modelLength;

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
        private final String catwalkId;
        private final String modelId;
        private final FieldIdsKey fieldIdsKey;
        private final MergedScores mergedScores;

        public ReadRepair(MiruTenantId tenantId, String catwalkId, String modelId, FieldIdsKey fieldIdsKey, MergedScores mergedScores) {
            this.tenantId = tenantId;
            this.catwalkId = catwalkId;
            this.modelId = modelId;
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

                if (merged != null) {
                    saveModel(tenantId,
                        catwalkId,
                        modelId,
                        merged.fromPartitionId,
                        merged.toPartitionId,
                        new String[][]{fieldIdsKey.fieldIds},
                        new ModelFeatureScores[]{new ModelFeatureScores(true, mergedScores.scores.featureScores, mergedScores.timeRange)});
                    removeModel(tenantId, catwalkId, modelId, mergedScores.ranges);
                }
            } catch (Exception x) {
                LOG.error("Failure while trying to apply read repairs.");
            }
        }

    }

    private static final FeatureScoreComparator FEATURE_SCORE_COMPARATOR = new FeatureScoreComparator();

    private static class FeatureScoreComparator implements Comparator<FeatureScore> {

        @Override
        public int compare(FeatureScore o1, FeatureScore o2) {
            int c = Integer.compare(o1.termIds.length, o2.termIds.length);
            if (c != 0) {
                return c;
            }
            for (int j = 0; j < o1.termIds.length; j++) {
                c = o1.termIds[j].compareTo(o2.termIds[j]);
                if (c != 0) {
                    return c;
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

        boolean contiguousClosedPartitions = true;

        FeatureRange latestRange;
        ModelFeatureScores latestScores;

        List<FeatureRange> ranges;
        ModelFeatureScores scores;
        MiruTimeRange timeRange;

        public MergedScores(FeatureRange latestRange, ModelFeatureScores latestScores) {
            this.latestRange = latestRange;
            this.latestScores = latestScores;
        }

    }

}
