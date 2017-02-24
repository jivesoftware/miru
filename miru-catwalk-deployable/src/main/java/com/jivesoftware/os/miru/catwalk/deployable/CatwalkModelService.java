package com.jivesoftware.os.miru.catwalk.deployable;

import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.MinMaxPriorityQueue;
import com.google.common.collect.PeekingIterator;
import com.google.common.collect.Sets;
import com.google.common.primitives.Longs;
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
import com.jivesoftware.os.miru.stream.plugins.catwalk.CatwalkQuery.CatwalkFeature;
import com.jivesoftware.os.miru.stream.plugins.catwalk.FeatureScore;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
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
        false, Consistency.leader_quorum, true, true, false, RowType.snappy_primary, "lab", -1, null, -1, -1);

    private final CatwalkModelQueue modelQueue;
    private final ExecutorService readRepairers;
    private final PartitionClientProvider clientProvider;
    private final MiruStats stats;
    private final float repairMinFeatureScore;
    private final float gatherMinFeatureScore;
    private final int repairMaxFeatureScoresPerFeature;
    private final int gatherMaxFeatureScoresPerFeature;
    private final boolean useScanCompression;

    private final long additionalSolverAfterNMillis;
    private final long abandonLeaderSolutionAfterNMillis;
    private final long abandonSolutionAfterNMillis;

    public CatwalkModelService(CatwalkModelQueue modelQueue,
        ExecutorService readRepairers,
        PartitionClientProvider clientProvider,
        MiruStats stats,
        float repairMinFeatureScore,
        float gatherMinFeatureScore,
        int repairMaxFeatureScoresPerFeature,
        int gatherMaxFeatureScoresPerFeature,
        boolean useScanCompression,
        long additionalSolverAfterNMillis,
        long abandonLeaderSolutionAfterNMillis,
        long abandonSolutionAfterNMillis) {
        this.modelQueue = modelQueue;
        this.readRepairers = readRepairers;
        this.clientProvider = clientProvider;
        this.stats = stats;
        this.repairMinFeatureScore = repairMinFeatureScore;
        this.gatherMinFeatureScore = gatherMinFeatureScore;
        this.repairMaxFeatureScoresPerFeature = repairMaxFeatureScoresPerFeature;
        this.gatherMaxFeatureScoresPerFeature = gatherMaxFeatureScoresPerFeature;
        this.useScanCompression = useScanCompression;
        this.additionalSolverAfterNMillis = additionalSolverAfterNMillis;
        this.abandonLeaderSolutionAfterNMillis = abandonLeaderSolutionAfterNMillis;
        this.abandonSolutionAfterNMillis = abandonSolutionAfterNMillis;
    }

    public Map<String, MergedScores> gatherModel(MiruTenantId tenantId,
        String catwalkId,
        String modelId,
        CatwalkFeature[] features,
        TreeSet<Integer> partitionIds,
        List<FeatureRange> deletableRanges) throws Exception {

        PartitionClient client = modelClient(tenantId);
        FeatureRange[] currentRange = { null };

        Map<String, MergedScores> fieldIdsToFeatureScores = new HashMap<>();
        client.scan(Consistency.leader_quorum,
            useScanCompression,
            prefixedKeyRangeStream -> {
                for (int i = 0; i < features.length; i++) {
                    String featureName = features[i].name;
                    byte[] fromKey = modelPartitionKey(catwalkId, modelId, featureName, 0, Integer.MAX_VALUE);
                    byte[] toKey = modelPartitionKey(catwalkId, modelId, featureName, Integer.MAX_VALUE, 0);
                    if (!prefixedKeyRangeStream.stream(null, fromKey, null, toKey)) {
                        return false;
                    }
                }
                return true;
            },
            (prefix, key, value, timestamp, version) -> {

                FeatureRange range = getFeatureRange(key, features);
                if (currentRange[0] == null || !currentRange[0].intersects(range)) {
                    currentRange[0] = range;

                    for (int partitionId = range.fromPartitionId; partitionId <= range.toPartitionId; partitionId++) {
                        partitionIds.add(partitionId);
                    }

                    String featureName = range.featureName;
                    int featureId = range.featureId;
                    ModelFeatureScores scores = valueFromBytes(value, featureId);
                    if (scores.timeRange != null) {
                        fieldIdsToFeatureScores.compute(featureName, (t, currentMerged) -> {
                            if (currentMerged == null) {
                                MergedScores mergedScores = new MergedScores(range, scores);
                                mergedScores.allRanges.add(range);
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
                            currentMerged.allRanges.add(range);
                            currentMerged.mergedRange = new FeatureRange(range.featureName,
                                featureId, Math.min(range.fromPartitionId, currentMerged.mergedRange.fromPartitionId),
                                Math.min(range.toPartitionId, currentMerged.mergedRange.toPartitionId));
                            currentMerged.mergedScores = mergedScores;
                            return currentMerged;
                        });
                    }
                } else {
                    deletableRanges.add(range);
                }
                return true;
            },
            additionalSolverAfterNMillis,
            abandonLeaderSolutionAfterNMillis,
            abandonSolutionAfterNMillis,
            Optional.<List<String>>empty());

        return fieldIdsToFeatureScores;
    }

    public CatwalkModel getModel(MiruTenantId tenantId, String catwalkId, String modelId, CatwalkQuery catwalkQuery) throws Exception {
        long start = System.currentTimeMillis();
        int modelCount = 0;
        try {
            List<FeatureRange> deletableRanges = Lists.newArrayList();
            TreeSet<Integer> partitionIds = Sets.newTreeSet();

            CatwalkFeature[] features = catwalkQuery.features;
            int numeratorsCount = catwalkQuery.gatherFilters.length;
            Map<String, MergedScores> featureNameToMergedScores = gatherModel(tenantId, catwalkId, modelId, features, partitionIds, deletableRanges);

            long[] modelCounts = new long[features.length];
            long totalCount = 0;
            int[] numberOfModels = new int[features.length];
            int[] totalNumPartitions = new int[features.length];

            @SuppressWarnings("unchecked")
            List<FeatureScore>[] featureScores = new List[features.length];
            int scoreCount = 0;
            int dropCount = 0;
            int existingCount = 0;
            int missingCount = 0;
            for (int i = 0; i < features.length; i++) {
                MergedScores mergedScores = featureNameToMergedScores.get(features[i].name);
                if (mergedScores != null) {
                    int featureModels = 1 + mergedScores.numberOfMerges;

                    modelCounts[i] = mergedScores.mergedScores.modelCount;
                    totalCount = Math.max(totalCount, mergedScores.mergedScores.totalCount);
                    numberOfModels[i] = featureModels;

                    for (FeatureRange allRange : mergedScores.allRanges) {
                        totalNumPartitions[i] += allRange.toPartitionId - allRange.fromPartitionId + 1;
                    }

                    if (mergedScores.mergedScores.featureScores.size() > gatherMaxFeatureScoresPerFeature) {
                        MinMaxPriorityQueue<FeatureScore> topFeatures = MinMaxPriorityQueue
                            .orderedBy(FEATURE_SCORES_PER_FEATURE_COMPARATOR)
                            .expectedSize(gatherMaxFeatureScoresPerFeature)
                            .maximumSize(gatherMaxFeatureScoresPerFeature)
                            .create();

                        if (gatherMinFeatureScore > 0f) {
                            topFeatures.addAll(filterEligibleScores(mergedScores.mergedScores.featureScores, gatherMinFeatureScore));
                        } else {
                            topFeatures.addAll(mergedScores.mergedScores.featureScores);
                        }
                        featureScores[i] = Lists.newArrayList(topFeatures);
                    } else if (gatherMinFeatureScore > 0f) {
                        featureScores[i] = filterEligibleScores(mergedScores.mergedScores.featureScores, gatherMinFeatureScore);
                    } else {
                        featureScores[i] = mergedScores.mergedScores.featureScores;
                    }

                    scoreCount += featureScores[i].size();
                    dropCount += (mergedScores.mergedScores.featureScores.size() - featureScores[i].size());
                    existingCount++;
                    modelCount += featureModels;
                } else {
                    featureScores[i] = Collections.emptyList();
                    missingCount++;
                }
            }
            LOG.info("Gathered scores:{} dropped:{} for tenantId:{} catwalkId:{} modelId:{} existing:{} missing:{} from {} models",
                scoreCount, dropCount, tenantId, catwalkId, modelId, existingCount, missingCount, modelCount);

            for (Map.Entry<String, MergedScores> entry : featureNameToMergedScores.entrySet()) {
                List<FeatureRange> ranges = entry.getValue().ranges;
                if (ranges != null && ranges.size() > 1) {
                    readRepairers.submit(new ReadRepair(tenantId, catwalkId, modelId, entry.getKey(), numeratorsCount, entry.getValue()));
                }
            }

            if (!deletableRanges.isEmpty()) {
                readRepairers.submit(() -> {
                    try {
                        removeModel(tenantId, catwalkId, modelId, deletableRanges);
                    } catch (Exception x) {
                        LOG.error("Failure while trying to delete.", x);
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

            return new CatwalkModel(modelCounts, totalCount, numberOfModels, featureScores, totalNumPartitions);
        } finally {
            long latency = System.currentTimeMillis() - start;
            stats.ingressed("service>model>get", 1, latency);
            stats.ingressed("service>model>get>" + tenantId.toString(), 1, latency);
            stats.ingressed("service>model>count", modelCount, latency);
            stats.ingressed("service>model>count>" + tenantId.toString(), modelCount, latency);
        }
    }

    private List<FeatureScore> filterEligibleScores(List<FeatureScore> featureScores, float minFeatureScore) {
        List<FeatureScore> eligibleScores = Lists.newArrayList();
        for (FeatureScore featureScore : featureScores) {
            for (long numerator : featureScore.numerators) {
                if (numerator > 0) {
                    float s = (float) numerator / featureScore.denominator;
                    if (s > minFeatureScore) {
                        eligibleScores.add(featureScore);
                        break;
                    }
                }
            }
        }
        return eligibleScores;
    }

    public void saveModel(MiruTenantId tenantId,
        String catwalkId,
        String modelId,
        int numeratorsCount,
        int fromPartitionId,
        int toPartitionId,
        String[] featureNames,
        ModelFeatureScores[] saveModels,
        float minFeatureScore,
        int maxFeatureScoresPerFeature) throws Exception {

        int modelCount = 0;
        int totalCount = 0;
        int scoreCount = 0;
        int dropCount = 0;
        ModelFeatureScores[] models = new ModelFeatureScores[featureNames.length]; // mutable copy
        if (saveModels != null) {
            for (int i = 0; i < saveModels.length; i++) {
                ModelFeatureScores initialModel = saveModels[i];

                List<FeatureScore> featureScores;
                if (initialModel.featureScores.size() > maxFeatureScoresPerFeature) {
                    MinMaxPriorityQueue<FeatureScore> topFeatures = MinMaxPriorityQueue
                        .orderedBy(FEATURE_SCORES_PER_FEATURE_COMPARATOR)
                        .expectedSize(maxFeatureScoresPerFeature)
                        .maximumSize(maxFeatureScoresPerFeature)
                        .create();

                    if (minFeatureScore > 0f) {
                        topFeatures.addAll(filterEligibleScores(initialModel.featureScores, minFeatureScore));
                    } else {
                        topFeatures.addAll(initialModel.featureScores);
                    }
                    featureScores = Lists.newArrayList(topFeatures);
                } else if (minFeatureScore > 0f) {
                    featureScores = filterEligibleScores(initialModel.featureScores, minFeatureScore);
                } else {
                    featureScores = initialModel.featureScores;
                }

                Collections.sort(featureScores, FEATURE_SCORE_COMPARATOR);

                models[i] = new ModelFeatureScores(initialModel.partitionIsClosed,
                    initialModel.modelCount,
                    initialModel.totalCount,
                    featureScores,
                    initialModel.timeRange);

                modelCount += initialModel.modelCount;
                totalCount += initialModel.totalCount;
                scoreCount += featureScores.size();
                dropCount += (initialModel.featureScores.size() - featureScores.size());
            }
        }

        LOG.info("Saving model for tenantId:{} catwalkId:{} modelId:{} from:{} to:{} modelCount:{} totalCount:{} scored:{} dropped:{}",
            tenantId, catwalkId, modelId, fromPartitionId, toPartitionId, modelCount, totalCount, scoreCount, dropCount);

        PartitionClient client = modelClient(tenantId);
        client.commit(Consistency.leader_quorum, null,
            commitKeyValueStream -> {
                for (int i = 0; i < featureNames.length; i++) {
                    byte[] key = modelPartitionKey(catwalkId, modelId, featureNames[i], fromPartitionId, toPartitionId);
                    byte[] value;
                    if (models[i] == null) {
                        value = valueToBytes(true, 0, 0, 0, Collections.emptyList(), null);
                    } else {
                        value = valueToBytes(models[i].partitionIsClosed,
                            models[i].modelCount,
                            models[i].totalCount,
                            numeratorsCount,
                            models[i].featureScores,
                            models[i].timeRange);
                    }
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
                    LOG.info("Removing model for tenantId:{} catwalkId:{} modelId:{} feature:{} from:{} to:{}",
                        tenantId, catwalkId, modelId, range.featureId, range.fromPartitionId, range.toPartitionId);
                    byte[] key = modelPartitionKey(catwalkId, modelId, range.featureName, range.fromPartitionId, range.toPartitionId);
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
        long[] numerators = new long[a.numerators.length];
        long denominator = a.denominator + b.denominator;
        for (int i = 0; i < a.numerators.length; i++) {
            numerators[i] = a.numerators[i] + b.numerators[i];
            if (numerators[i] > denominator) {
                LOG.warn("Merged numerator:{} denominator:{} for scores: {} {}", numerators[i], denominator, a, b);
            }
        }
        int numPartitions = a.numPartitions + b.numPartitions;
        return new FeatureScore(a.termIds, numerators, denominator, numPartitions);
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
    private long decay(long denominator, int totalNumPartitions, int numPartitions) {
        return (denominator * totalNumPartitions) / numPartitions;
    }

    static byte[] valueToBytes(boolean partitionIsClosed,
        long modelCount,
        long totalCount,
        int numeratorsCount,
        List<FeatureScore> scores,
        MiruTimeRange timeRange) throws IOException {

        HeapFiler filer = new HeapFiler(1 + 1 + 4 + 8 + 8 + 4 + scores.size() * (4 + 4 + 10 + 1 + (numeratorsCount * 8) + 8) + 8 + 8);

        UIO.writeByte(filer, (byte) 3, "version");
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
            UIO.writeByte(filer, (byte) score.numerators.length, "numeratorsCount");
            for (int i = 0; i < score.numerators.length; i++) {
                UIO.writeLong(filer, score.numerators[i], "numerator");
            }
            UIO.writeLong(filer, score.denominator, "denominator");
            UIO.writeInt(filer, score.numPartitions, "numPartitions", lengthBuffer);
        }

        UIO.writeLong(filer, timeRange == null ? -1 : timeRange.smallestTimestamp, "smallestTimestamp");
        UIO.writeLong(filer, timeRange == null ? -1 : timeRange.largestTimestamp, "largestTimestamp");

        return filer.getBytes();
    }

    static ModelFeatureScores valueFromBytes(byte[] value, int featureIndex) throws IOException {
        HeapFiler filer = HeapFiler.fromBytes(value, value.length);
        byte version = UIO.readByte(filer, "version");
        if (version < 1 || version > 3) {
            throw new IllegalStateException("Unexpected version " + version);
        }
        boolean partitionIsClosed = UIO.readByte(filer, "partitionIsClosed") == 1;

        byte[] lengthBuffer = new byte[8];

        long modelCount;
        if (version < 2) {
            int modelCountsLength = UIO.readInt(filer, "modelCountsLength", lengthBuffer);
            long[] modelCounts = new long[modelCountsLength];
            if (featureIndex >= modelCountsLength) {
                throw new IllegalStateException("Unexpected feature index:" + filer + " for length:" + modelCountsLength);
            }
            for (int i = 0; i < modelCountsLength; i++) {
                modelCounts[i] = UIO.readLong(filer, "modelCount", lengthBuffer);
            }
            modelCount = modelCounts[featureIndex];
        } else {
            modelCount = UIO.readLong(filer, "modelCount", lengthBuffer);
        }

        long totalCount = UIO.readLong(filer, "totalCount", lengthBuffer);
        int scoresLength = UIO.readInt(filer, "scoresLength", value);
        List<FeatureScore> scores = new ArrayList<>(scoresLength);
        for (int i = 0; i < scoresLength; i++) {
            MiruTermId[] terms = new MiruTermId[UIO.readInt(filer, "termsLength", value)];
            for (int j = 0; j < terms.length; j++) {
                terms[j] = new MiruTermId(UIO.readByteArray(filer, "term", lengthBuffer));
            }
            long[] numerators;
            if (version < 3) {
                numerators = new long[] { UIO.readLong(filer, "numerator", lengthBuffer) };
            } else {
                int numeratorCount = UIO.readByte(filer, "numeratorCount");
                numerators = new long[numeratorCount];
                for (int j = 0; j < numeratorCount; j++) {
                    numerators[j] = UIO.readLong(filer, "numerator", lengthBuffer);
                }
            }
            long denominator = UIO.readLong(filer, "denominator", lengthBuffer);
            int numPartitions = UIO.readInt(filer, "numPartitions", lengthBuffer);
            scores.add(new FeatureScore(terms, numerators, denominator, numPartitions));
        }

        long smallestTimestamp = UIO.readLong(filer, "smallestTimestamp", lengthBuffer);
        long largestTimestamp = UIO.readLong(filer, "largestTimestamp", lengthBuffer);
        MiruTimeRange timeRange = (smallestTimestamp == -1 && largestTimestamp == -1) ? null : new MiruTimeRange(
            smallestTimestamp,
            largestTimestamp);
        return new ModelFeatureScores(partitionIsClosed, modelCount, totalCount, scores, timeRange);
    }

    private PartitionClient modelClient(MiruTenantId tenantId) throws Exception {
        byte[] nameBytes = ("model-" + tenantId).getBytes(StandardCharsets.UTF_8);
        return clientProvider.getPartition(new PartitionName(false, nameBytes, nameBytes), 3, MODEL_PROPERTIES);
    }

    static byte[] modelPartitionKey(String catwalkId, String modelId, String featureName, int fromPartitionId, int toPartitionId) {
        byte[] catwalkBytes = catwalkId.getBytes(StandardCharsets.UTF_8);
        byte[] modelBytes = modelId.getBytes(StandardCharsets.UTF_8);
        byte[] featureNameBytes = featureName.getBytes(StandardCharsets.UTF_8);
        if (featureNameBytes.length > Byte.MAX_VALUE) {
            throw new IllegalArgumentException("Max feature name length is " + Byte.MAX_VALUE);
        }

        int fieldsSizeInByte = 0;
        fieldsSizeInByte += 1;
        fieldsSizeInByte += featureNameBytes.length;

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

        keyBytes[offset] = (byte) (featureNameBytes.length);
        offset++;

        UIO.writeBytes(featureNameBytes, keyBytes, offset);
        offset += featureNameBytes.length;

        UIO.intBytes(fromPartitionId, keyBytes, offset);
        offset += 4;

        // flip so highest toPartitionId sorts first (relative to fromPartitionId)
        UIO.intBytes(Integer.MAX_VALUE - toPartitionId, keyBytes, offset);
        offset += 4;

        return keyBytes;
    }

    static FeatureRange getFeatureRange(byte[] key, CatwalkFeature[] features) {

        int offset = 0;

        int catwalkLength = UIO.bytesShort(key, offset);
        offset += 2;
        offset += catwalkLength;

        int modelLength = UIO.bytesShort(key, offset);
        offset += 2;
        offset += modelLength;

        int fieldNameLength = key[offset];
        offset++;

        String featureName = new String(key, offset, fieldNameLength, StandardCharsets.UTF_8);
        offset += fieldNameLength;

        int fromPartitionId = UIO.bytesInt(key, offset);
        offset += 4;

        int toPartitionId = Integer.MAX_VALUE - UIO.bytesInt(key, offset);
        offset += 4;

        int featureId = -1;
        for (int i = 0; i < features.length; i++) {
            if (features[i].name.equals(featureName)) {
                featureId = i;
                break;
            }
        }

        return new FeatureRange(featureName, featureId, fromPartitionId, toPartitionId);
    }

    public static class FeatureRange {

        public final String featureName;
        public final int featureId;
        public final int fromPartitionId;
        public final int toPartitionId;

        public FeatureRange(String featureName, int featureId, int fromPartitionId, int toPartitionId) {
            this.featureName = featureName;
            this.featureId = featureId;
            this.fromPartitionId = fromPartitionId;
            this.toPartitionId = toPartitionId;
        }

        public boolean intersects(FeatureRange range) {
            if (!featureName.equals(range.featureName)) {
                return false;
            }
            return fromPartitionId <= range.toPartitionId && range.fromPartitionId <= toPartitionId;
        }

        private FeatureRange merge(FeatureRange range) {
            if (!featureName.equals(range.featureName)) {
                throw new IllegalStateException("Trying to merge ranges that are for different features " + featureName + " vs " + range.featureName);
            }
            return new FeatureRange(featureName, featureId, Math.min(fromPartitionId, range.fromPartitionId), Math.max(toPartitionId, range.toPartitionId));
        }
    }

    private class ReadRepair implements Runnable {

        private final MiruTenantId tenantId;
        private final String catwalkId;
        private final String modelId;
        private final String featureName;
        private final int numeratorsCount;
        private final MergedScores mergedScores;

        public ReadRepair(MiruTenantId tenantId, String catwalkId, String modelId, String featureName, int numeratorsCount, MergedScores mergedScores) {
            this.tenantId = tenantId;
            this.catwalkId = catwalkId;
            this.modelId = modelId;
            this.featureName = featureName;
            this.numeratorsCount = numeratorsCount;
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
                    LOG.info("Merging model for tenantId:{} catwalkId:{} modelId:{} feature:{} from:{} to:{}",
                        tenantId, catwalkId, modelId, merged.featureId, merged.fromPartitionId, merged.toPartitionId);
                    ModelFeatureScores modelFeatureScores = new ModelFeatureScores(true,
                        mergedScores.scores.modelCount,
                        mergedScores.scores.totalCount,
                        mergedScores.scores.featureScores,
                        mergedScores.timeRange);
                    saveModel(tenantId,
                        catwalkId,
                        modelId,
                        numeratorsCount,
                        merged.fromPartitionId,
                        merged.toPartitionId,
                        new String[] { featureName },
                        new ModelFeatureScores[] { modelFeatureScores },
                        repairMinFeatureScore,
                        repairMaxFeatureScoresPerFeature);
                    removeModel(tenantId, catwalkId, modelId, mergedScores.ranges);
                }
            } catch (Exception x) {
                LOG.error("Failure while trying to apply read repairs.", x);
            }
        }

    }

    private static final Comparator<FeatureScore> FEATURE_SCORES_PER_FEATURE_COMPARATOR = (o1, o2) -> {
        long n1 = Longs.max(o1.numerators);
        long n2 = Longs.max(o2.numerators);
        float s1 = (float) n1 / o1.denominator;
        float s2 = (float) n2 / o2.denominator;
        int c = Float.compare(s2, s1); // descending
        if (c != 0) {
            return c;
        }
        return Long.compare(o2.denominator, o1.denominator); // descending
    };

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

    public static class MergedScores {

        boolean contiguousClosedPartitions = true;
        int numberOfMerges = 0;

        final FeatureRange firstRange;
        FeatureRange mergedRange;
        public ModelFeatureScores mergedScores;

        final List<FeatureRange> ranges = new ArrayList<>();
        public final List<FeatureRange> allRanges = new ArrayList<>();
        ModelFeatureScores scores;
        MiruTimeRange timeRange;

        public MergedScores(FeatureRange mergedRange, ModelFeatureScores mergedScores) {
            this.firstRange = mergedRange;
            this.mergedRange = mergedRange;
            this.mergedScores = mergedScores;
        }

    }

}
