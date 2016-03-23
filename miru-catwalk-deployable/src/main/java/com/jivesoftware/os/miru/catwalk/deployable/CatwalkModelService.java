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
        FeatureRange[] currentRange = { null };
        String[][] featureFields = catwalkQuery.featureFields;
        client.scan(Consistency.leader_quorum,
            prefixedKeyRangeStream -> {
                for (int i = 0; i < featureFields.length; i++) {
                    String[] fields = featureFields[i];
                    byte[] fromKey = modelPartitionKey(catwalkId, modelId, fields, 0, Integer.MAX_VALUE);
                    byte[] toKey = modelPartitionKey(catwalkId, modelId, fields, Integer.MAX_VALUE, 0);
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
                            MergedScores mergedScores = new MergedScores(range, scores);
                            if (scores.partitionIsClosed) {
                                mergedScores.ranges.add(range);
                                mergedScores.scores = scores;
                                mergedScores.timeRange = scores.timeRange;
                            }
                            return mergedScores;
                        }

                        PeekingIterator<FeatureScore> a = Iterators.peekingIterator(currentMerged.mergedScores.featureScores.iterator());
                        PeekingIterator<FeatureScore> b = Iterators.peekingIterator(scores.featureScores.iterator());

                        // dur: 9  5  7  6  5  7  9  4
                        // pid: 0, 1, 2, 3, 4, 5, 6, 7

                        // 0*0.9, 1...
                        // 01*0.9, 2...
                        // 02*0.9, 3...

                        // 06 -> A-view-B -> 21 / 70 = (3 / 10) / partition
                        //  7 -> A-view-B -> 0 / 10
                        // 06, 7... -> 21/80

                        // 06, 7, 8
                        // 07, 8
                        // 07*0.9, 8


                        // 07, A-view-B ->   99 / 100
                        // 8 9 10 11 12 13 157, A-view-B -> ???????
                        // 158, A-view-B -> 99 / 100
                        // 0..158, A-view-B -> 198 / 200

                        List<FeatureScore> merged = new ArrayList<>(currentMerged.mergedScores.featureScores.size() + scores.featureScores.size());
                        while (a.hasNext() || b.hasNext()) {

                            if (a.hasNext() && b.hasNext()) {
                                int c = FEATURE_SCORE_COMPARATOR.compare(a.peek(), b.peek());
                                if (c == 0) {
                                    merged.add(merge(a.next(), b.next()));
                                } else if (c < 0) {
                                    merged.add(decay(a.next(), currentMerged.mergedRange.toPartitionId - currentMerged.firstRange.fromPartitionId + 1));
                                } else {
                                    merged.add(b.next());
                                }
                            } else if (a.hasNext()) {
                                merged.add(decay(a.next(), currentMerged.mergedRange.toPartitionId - currentMerged.firstRange.fromPartitionId + 1));

                            } else if (b.hasNext()) {
                                merged.add(b.next());
                            }
                        }

                        ModelFeatureScores mergedScores = new ModelFeatureScores(scores.partitionIsClosed,
                            currentMerged.mergedScores.modelCount + scores.modelCount,
                            currentMerged.mergedScores.totalCount + scores.totalCount,
                            merged,
                            scores.timeRange);

                        currentMerged.contiguousClosedPartitions &= scores.partitionIsClosed;
                        currentMerged.contiguousClosedPartitions &= (currentMerged.mergedRange.toPartitionId + 1 == range.fromPartitionId);
                        if (currentMerged.contiguousClosedPartitions) {
                            currentMerged.ranges.add(range);
                            currentMerged.scores = mergedScores;
                            if (currentMerged.timeRange == null) {
                                currentMerged.timeRange = scores.timeRange;
                            } else {
                                currentMerged.timeRange = new MiruTimeRange(
                                    Math.min(scores.timeRange.smallestTimestamp, currentMerged.timeRange.smallestTimestamp),
                                    Math.max(scores.timeRange.largestTimestamp, currentMerged.timeRange.largestTimestamp));
                            }
                        }

                        currentMerged.numberOfMerges++;
                        currentMerged.mergedRange = new FeatureRange(range.fieldIds,
                            Math.min(range.fromPartitionId, currentMerged.mergedRange.fromPartitionId),
                            Math.min(range.toPartitionId, currentMerged.mergedRange.toPartitionId));
                        currentMerged.mergedScores = mergedScores;
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

        long modelCount = 0;
        long totalCount = 0;
        int[] numberOfModels = new int[featureFields.length];

        @SuppressWarnings("unchecked")
        List<FeatureScore>[] featureScores = new List[featureFields.length];
        for (int i = 0; i < featureFields.length; i++) {
            MergedScores mergedScores = fieldIdsToFeatureScores.get(new FieldIdsKey(featureFields[i]));
            if (mergedScores != null) {
                int featureModels = 1 + mergedScores.numberOfMerges;

                modelCount = Math.max(modelCount, mergedScores.mergedScores.modelCount);
                totalCount = Math.max(totalCount, mergedScores.mergedScores.totalCount);
                numberOfModels[i] = featureModels;

                featureScores[i] = mergedScores.mergedScores.featureScores;
                LOG.info("Gathered {} scores for tenantId:{} catwalkId:{} modelId:{} feature:{} from {} models",
                    featureScores[i].size(), tenantId, catwalkId, modelId, i, featureModels);
            } else {
                featureScores[i] = Collections.emptyList();
                LOG.info("Gathered no scores for tenantId:{} catwalkId:{} modelId:{} feature:{}",
                    featureScores[i].size(), tenantId, catwalkId, modelId, i);
            }
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
                LOG.info("Requesting repair for missing partitionId:{} for tenantId:{} catwalkId:{} modelId:{}",
                    partitionId, tenantId, catwalkId, modelId);
                modelQueue.enqueue(tenantId, catwalkId, modelId, partitionId, catwalkQuery);
            }
        }

        CatwalkModel model = new CatwalkModel(modelCount, totalCount, numberOfModels, featureScores);
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

        LOG.info("Saving model for tenantId:{} catwalkId:{} modelId:{} from:{} to:{}",
            tenantId, catwalkId, modelId, fromPartitionId, toPartitionId);

        for (ModelFeatureScores model : models) {
            Collections.sort(model.featureScores, FEATURE_SCORE_COMPARATOR);
        }

        PartitionClient client = modelClient(tenantId);
        client.commit(Consistency.leader_quorum, null,
            commitKeyValueStream -> {
                for (int i = 0; i < featureFields.length; i++) {
                    byte[] key = modelPartitionKey(catwalkId, modelId, featureFields[i], fromPartitionId, toPartitionId);
                    byte[] value = valueToBytes(models[i].partitionIsClosed,
                        models[i].modelCount,
                        models[i].totalCount,
                        models[i].featureScores,
                        models[i].timeRange);
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
                    LOG.info("Removing model for tenantId:{} catwalkId:{} modelId:{} fieldIds:{} from:{} to:{}",
                        tenantId, catwalkId, modelId, Arrays.toString(range.fieldIds), range.fromPartitionId, range.toPartitionId);
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
        long numerator = a.numerator + b.numerator;
        long denominator = a.denominator + b.denominator;
        if (numerator > denominator) {
            LOG.warn("Merged numerator:{} denominator:{} for scores: {} {}", numerator, denominator, a, b);
        }
        return new FeatureScore(a.termIds, numerator, denominator);
    }

    //  0 ->  25 / 73
    //  1 ->  25 / 22
    // 01 ->  50 / 95
    //  2 ->   0 / ?  = 0 / (95/2p) = 0 / 47
    // 02 ->  50 / 142
    //  3 ->  25 / 65
    // 03 ->  75 / 207
    //  4 ->  25 / 26
    // 04 -> 100 / 233

    private FeatureScore decay(FeatureScore score, int numPartitions) {
        return new FeatureScore(score.termIds, score.numerator, score.denominator + (score.denominator / numPartitions));
    }

    static byte[] valueToBytes(boolean partitionIsClosed,
        long modelCount,
        long totalCount,
        List<FeatureScore> scores,
        MiruTimeRange timeRange) throws IOException {

        HeapFiler filer = new HeapFiler(1 + 1 + 8 + 8 + scores.size() * (8 + 8 + 4 + 4 + 10) + 8 + 8); //TODO rough guesstimation

        UIO.writeByte(filer, (byte) 0, "version");
        UIO.writeByte(filer, partitionIsClosed ? (byte) 1 : (byte) 0, "partitionIsClosed");

        UIO.writeLong(filer, modelCount, "modelCount");
        UIO.writeLong(filer, totalCount, "totalCount");

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

    static ModelFeatureScores valueFromBytes(byte[] value) throws IOException {
        HeapFiler filer = HeapFiler.fromBytes(value, value.length);
        byte version = UIO.readByte(filer, "version");
        if (version != 0) {
            throw new IllegalStateException("Unexpected version " + version);
        }
        boolean partitionIsClosed = UIO.readByte(filer, "partitionIsClosed") == 1;

        byte[] lengthBuffer = new byte[8];
        long modelCount = UIO.readLong(filer, "modelCount", lengthBuffer);
        long totalCount = UIO.readLong(filer, "totalCount", lengthBuffer);
        int scoresLength = UIO.readInt(filer, "scoresLength", value);
        List<FeatureScore> scores = new ArrayList<>(scoresLength);
        for (int i = 0; i < scoresLength; i++) {
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
        return new ModelFeatureScores(partitionIsClosed, modelCount, totalCount, scores, timeRange);
    }

    private PartitionClient modelClient(MiruTenantId tenantId) throws Exception {
        byte[] nameBytes = ("model-" + tenantId).getBytes(StandardCharsets.UTF_8);
        return clientProvider.getPartition(new PartitionName(false, nameBytes, nameBytes), 3, MODEL_PROPERTIES);
    }

    static byte[] modelPartitionKey(String catwalkId, String modelId, String[] fields, int fromPartitionId, int toPartitionId) {
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

        UIO.shortBytes((short) catwalkBytes.length, keyBytes, offset);
        offset += 2;

        UIO.writeBytes(catwalkBytes, keyBytes, offset);
        offset += catwalkBytes.length;

        UIO.shortBytes((short) modelBytes.length, keyBytes, offset);
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

    static FeatureRange getFeatureRange(byte[] key) {

        int offset = 0;

        int catwalkLength = UIO.bytesShort(key, offset);
        offset += 2;
        offset += catwalkLength;

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

    static class FeatureRange {

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
                    LOG.info("Merging model for tenantId:{} catwalkId:{} modelId:{} from:{} to:{}",
                        tenantId, catwalkId, modelId, merged.fromPartitionId, merged.toPartitionId);
                    ModelFeatureScores modelFeatureScores = new ModelFeatureScores(true,
                        mergedScores.scores.modelCount,
                        mergedScores.scores.totalCount,
                        mergedScores.scores.featureScores,
                        mergedScores.timeRange);
                    saveModel(tenantId,
                        catwalkId,
                        modelId,
                        merged.fromPartitionId,
                        merged.toPartitionId,
                        new String[][] { fieldIdsKey.fieldIds },
                        new ModelFeatureScores[] { modelFeatureScores });
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
        int numberOfMerges = 0;

        final FeatureRange firstRange;
        FeatureRange mergedRange;
        ModelFeatureScores mergedScores;

        final List<FeatureRange> ranges = new ArrayList<>();
        ModelFeatureScores scores;
        MiruTimeRange timeRange;

        public MergedScores(FeatureRange mergedRange, ModelFeatureScores mergedScores) {
            this.firstRange = mergedRange;
            this.mergedRange = mergedRange;
            this.mergedScores = mergedScores;
        }

    }

}
