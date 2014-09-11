package com.jivesoftware.os.miru.service;

import com.google.common.base.Charsets;
import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Collections2;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Lists;
import com.jivesoftware.os.jive.utils.logger.MetricLogger;
import com.jivesoftware.os.jive.utils.logger.MetricLoggerFactory;
import com.jivesoftware.os.miru.api.MiruBackingStorage;
import com.jivesoftware.os.miru.api.MiruHost;
import com.jivesoftware.os.miru.api.MiruPartition;
import com.jivesoftware.os.miru.api.MiruPartitionCoord;
import com.jivesoftware.os.miru.api.MiruPartitionCoordInfo;
import com.jivesoftware.os.miru.api.activity.MiruPartitionId;
import com.jivesoftware.os.miru.api.activity.MiruPartitionedActivity;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.api.base.MiruTermId;
import com.jivesoftware.os.miru.cluster.MiruActivityLookupTable;
import com.jivesoftware.os.miru.query.Miru;
import com.jivesoftware.os.miru.query.bitmap.MiruBitmapsDebug;
import com.jivesoftware.os.miru.query.context.MiruRequestContext;
import com.jivesoftware.os.miru.query.index.MiruField;
import com.jivesoftware.os.miru.query.index.MiruInvertedIndex;
import com.jivesoftware.os.miru.query.partition.MiruHostedPartition;
import com.jivesoftware.os.miru.query.partition.MiruPartitionDirector;
import com.jivesoftware.os.miru.query.partition.OrderedPartitions;
import com.jivesoftware.os.miru.query.solution.MiruAnswerEvaluator;
import com.jivesoftware.os.miru.query.solution.MiruAnswerMerger;
import com.jivesoftware.os.miru.query.solution.MiruRequestHandle;
import com.jivesoftware.os.miru.query.solution.MiruResponse;
import com.jivesoftware.os.miru.query.solution.MiruSolution;
import com.jivesoftware.os.miru.query.solution.MiruSolvable;
import com.jivesoftware.os.miru.query.solution.MiruSolvableFactory;
import com.jivesoftware.os.miru.service.partition.MiruHostedPartitionComparison;
import com.jivesoftware.os.miru.service.solver.MiruSolved;
import com.jivesoftware.os.miru.service.solver.MiruSolver;
import com.jivesoftware.os.miru.wal.activity.MiruActivityWALWriter;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.Callable;

/**
 * @author jonathan
 */
public class MiruService implements Miru {

    private static final MetricLogger log = MetricLoggerFactory.getLogger();

    private final MiruHost localhost;
    private final MiruPartitionDirector partitionDirector;
    private final MiruSolver solver;
    private final MiruHostedPartitionComparison partitionComparison;
    private final MiruActivityWALWriter activityWALWriter;
    private final MiruActivityLookupTable activityLookupTable;
    private final MiruBitmapsDebug bitmapsDebug = new MiruBitmapsDebug();

    public MiruService(MiruHost localhost,
            MiruPartitionDirector partitionDirector,
            MiruHostedPartitionComparison partitionComparison,
            MiruActivityWALWriter activityWALWriter,
            MiruActivityLookupTable activityLookupTable,
            MiruSolver solver) {

        this.localhost = localhost;
        this.partitionDirector = partitionDirector;
        this.partitionComparison = partitionComparison;
        this.activityWALWriter = activityWALWriter;
        this.activityLookupTable = activityLookupTable;
        this.solver = solver;
    }

    public void writeToIndex(List<MiruPartitionedActivity> partitionedActivities) throws Exception {
        ListMultimap<MiruTenantId, MiruPartitionedActivity> perTenantPartitionedActivities = ArrayListMultimap.create();
        for (MiruPartitionedActivity partitionedActivity : partitionedActivities) {
            perTenantPartitionedActivities.put(partitionedActivity.tenantId, partitionedActivity);
        }
        partitionDirector.index(perTenantPartitionedActivities);
    }

    public void writeWAL(List<MiruPartitionedActivity> partitionedActivities) throws Exception {
        ListMultimap<MiruTenantId, MiruPartitionedActivity> perTenantPartitionedActivities = ArrayListMultimap.create();
        for (MiruPartitionedActivity partitionedActivity : partitionedActivities) {
            perTenantPartitionedActivities.put(partitionedActivity.tenantId, partitionedActivity);
        }
        for (MiruTenantId tenantId : perTenantPartitionedActivities.keySet()) {
            List<MiruPartitionedActivity> tenantPartitionedActivities = perTenantPartitionedActivities.get(tenantId);
            activityWALWriter.write(tenantId, tenantPartitionedActivities);
            activityLookupTable.add(tenantId, tenantPartitionedActivities);
        }
    }

    public long sizeInBytes() {
        return -1;
    }

    @Override
    public <A, P> MiruResponse<A> askAndMerge(
            MiruTenantId tenantId,
            final MiruSolvableFactory<A, P> solvableFactory,
            MiruAnswerEvaluator<A> evaluator,
            MiruAnswerMerger<A> merger,
            A defaultValue) throws Exception {

        log.startTimer("askAndMerge");

        A answer = null;
        List<MiruSolution> solutions = Lists.newArrayList();
        long totalElapsed;

        try {
            Iterable<OrderedPartitions> partitionReplicas = partitionDirector.allQueryablePartitionsInOrder(tenantId);

            Optional<A> lastAnswer = Optional.absent();

            for (OrderedPartitions orderedPartitions : partitionReplicas) {

                final Optional<A> optionalAnswer = lastAnswer;
                Collection<MiruSolvable<A>> solvables = Collections2.transform(orderedPartitions.partitions,
                        new Function<MiruHostedPartition<?>, MiruSolvable<A>>() {
                            @Override
                            public MiruSolvable<A> apply(final MiruHostedPartition<?> replica) {
                                return solvableFactory.create(replica, solvableFactory.getReport(optionalAnswer));
                            }
                        });
                List<MiruPartition> ordered = Lists.transform(orderedPartitions.partitions, new Function<MiruHostedPartition<?>, MiruPartition>() {
                    @Override
                    public MiruPartition apply(MiruHostedPartition<?> input) {
                        return new MiruPartition(input.getCoord(), new MiruPartitionCoordInfo(input.getState(), input.getStorage()));
                    }
                });

                Optional<Long> suggestedTimeoutInMillis = partitionComparison.suggestTimeout(orderedPartitions.tenantId, orderedPartitions.partitionId,
                        solvableFactory.getQueryKey());
                MiruSolved<A> solved = solver.solve(solvables.iterator(), suggestedTimeoutInMillis, ordered);

                if (solved == null) {
                    // fatal timeout
                    //TODO annotate answer to indicate partial failure
                    break;
                }

                solutions.add(solved.solution);

                A currentAnswer = solved.answer;
                A merged = merger.merge(lastAnswer, currentAnswer);

                lastAnswer = Optional.of(merged);
                if (evaluator.isDone(merged)) {
                    break;
                }
            }

            partitionComparison.analyzeSolutions(solutions, solvableFactory.getQueryKey());

            answer = merger.done(lastAnswer, defaultValue);

        } finally {
            totalElapsed = log.stopTimer("askAndMerge");
        }

        log.inc("askAndMerge>all");
        log.inc("askAndMerge>tenant>" + tenantId);
        log.inc("askAndMerge>query>" + solvableFactory.getQueryKey());
        log.inc("askAndMerge>tenantAndQuery>" + tenantId + '>' + solvableFactory.getQueryKey());

        return new MiruResponse<>(answer, solutions, totalElapsed);
    }

    @Override
    public <A, P> A askImmediate(
            MiruTenantId tenantId,
            MiruPartitionId partitionId,
            MiruSolvableFactory<A, P> solvableFactory,
            Optional<P> report,
            A defaultValue) throws Exception {
        Optional<MiruHostedPartition<?>> partition = getLocalTenantPartition(tenantId, partitionId);

        if (partition.isPresent()) {
            Callable<A> callable = solvableFactory.create(partition.get(), report);
            A answer = callable.call();

            log.inc("askImmediate>all");
            log.inc("askImmediate>tenant>" + tenantId);
            log.inc("askImmediate>query>" + solvableFactory.getQueryKey());
            log.inc("askImmediate>tenantAndQuery>" + tenantId + '>' + solvableFactory.getQueryKey());

            return answer;
        } else {
            return defaultValue;
        }
    }

    /**
     * Proactively warm a tenant for immediate use.
     */
    public void warm(MiruTenantId tenantId) throws Exception {
        partitionDirector.warm(tenantId);
    }

    /**
     * Inspect a field term.
     */
    public String inspect(MiruTenantId tenantId, MiruPartitionId partitionId, String fieldName, String termValue) throws Exception {
        Optional<MiruHostedPartition<?>> partition = getLocalTenantPartition(tenantId, partitionId);
        if (partition.isPresent()) {
            return inspect(partition.get(), fieldName, termValue);
        } else {
            return "Partition unavailable";
        }
    }

    private <BM> String inspect(MiruHostedPartition<BM> partition, String fieldName, String termValue) throws Exception {
        try (MiruRequestHandle<BM> handle = partition.getQueryHandle()) {
            MiruRequestContext<BM> requestContext = handle.getRequestContext();
            int fieldId = requestContext.schema.getFieldId(fieldName);
            MiruField<BM> field = requestContext.fieldIndex.getField(fieldId);
            Optional<? extends MiruInvertedIndex<BM>> invertedIndex = field.getInvertedIndex(new MiruTermId(termValue.getBytes(Charsets.UTF_8)));
            if (invertedIndex.isPresent()) {
                return bitmapsDebug.toString(handle.getBitmaps(), invertedIndex.get().getIndex());
            } else {
                return "Index not present";
            }
        }
    }

    /**
     * Manage topology and configuration.
     */
    public void setStorage(MiruTenantId tenantId, MiruPartitionId partitionId, MiruBackingStorage storage) throws Exception {
        partitionDirector.setStorage(tenantId, partitionId, storage);
    }

    public void removeHost(MiruHost host) throws Exception {
        partitionDirector.removeHost(host);
    }

    public void removeReplicas(MiruTenantId tenantId, MiruPartitionId partitionId) throws Exception {
        partitionDirector.removeReplicas(tenantId, partitionId);
    }

    public void moveReplica(MiruTenantId tenantId, MiruPartitionId partitionId, Optional<MiruHost> fromHost) throws Exception {
        partitionDirector.moveReplica(tenantId, partitionId, fromHost, localhost);
    }

    public void removeTopology(MiruTenantId tenantId, MiruPartitionId partitionId, MiruHost host) throws Exception {
        partitionDirector.removeTopology(tenantId, partitionId, host);
    }

    public boolean checkInfo(MiruTenantId tenantId, MiruPartitionId partitionId, MiruPartitionCoordInfo info) {
        return partitionDirector.checkInfo(tenantId, partitionId, info);
    }

    private Optional<MiruHostedPartition<?>> getLocalTenantPartition(MiruTenantId tenantId, MiruPartitionId partitionId) {
        MiruPartitionCoord localPartitionCoord = new MiruPartitionCoord(tenantId, partitionId, localhost);
        return partitionDirector.getQueryablePartition(localPartitionCoord);
    }

}
