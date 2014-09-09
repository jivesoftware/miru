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
import com.jivesoftware.os.miru.api.MiruPartitionCoord;
import com.jivesoftware.os.miru.api.MiruPartitionCoordInfo;
import com.jivesoftware.os.miru.api.activity.MiruPartitionId;
import com.jivesoftware.os.miru.api.activity.MiruPartitionedActivity;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.api.base.MiruTermId;
import com.jivesoftware.os.miru.cluster.MiruActivityLookupTable;
import com.jivesoftware.os.miru.query.Miru;
import com.jivesoftware.os.miru.query.MiruBitmapsDebug;
import com.jivesoftware.os.miru.query.MiruField;
import com.jivesoftware.os.miru.query.MiruHostedPartition;
import com.jivesoftware.os.miru.query.MiruInvertedIndex;
import com.jivesoftware.os.miru.query.MiruPartitionDirector;
import com.jivesoftware.os.miru.query.MiruQueryHandle;
import com.jivesoftware.os.miru.query.MiruQueryStream;
import com.jivesoftware.os.miru.query.MiruResultEvaluator;
import com.jivesoftware.os.miru.query.MiruResultMerger;
import com.jivesoftware.os.miru.query.MiruSolvable;
import com.jivesoftware.os.miru.query.MiruSolvableFactory;
import com.jivesoftware.os.miru.query.OrderedPartitions;
import com.jivesoftware.os.miru.service.partition.MiruHostedPartitionComparison;
import com.jivesoftware.os.miru.service.stream.factory.MiruSolution;
import com.jivesoftware.os.miru.service.stream.factory.MiruSolver;
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
    public <R, P> R callAndMerge(
            MiruTenantId tenantId,
            final MiruSolvableFactory<R, P> solvableFactory,
            MiruResultEvaluator<R> evaluator,
            MiruResultMerger<R> merger,
            R defaultValue) throws Exception {
        Iterable<OrderedPartitions> partitionReplicas = partitionDirector.allQueryablePartitionsInOrder(tenantId);

        Optional<R> lastResult = Optional.absent();
        int numSearchedPartitions = 0;
        List<MiruSolution<R>> solutions = Lists.newArrayList();
        for (OrderedPartitions orderedPartitions : partitionReplicas) {

            final Optional<R> result = lastResult;
            Collection<MiruSolvable<R>> solvables = Collections2.transform(orderedPartitions.partitions,
                    new Function<MiruHostedPartition<?>, MiruSolvable<R>>() {
                        @Override
                        public MiruSolvable<R> apply(final MiruHostedPartition<?> replica) {
                            return solvableFactory.create(replica, result);
                        }
                    });

            Optional<Long> suggestedTimeoutInMillis = partitionComparison.suggestTimeout(orderedPartitions.tenantId, orderedPartitions.partitionId,
                    solvableFactory.getQueryKey());
            MiruSolution<R> solution = solver.solve(solvables.iterator(), suggestedTimeoutInMillis);

            numSearchedPartitions++;

            if (solution == null) {
                // fatal timeout
                //TODO annotate result to indicate partial failure
                break;
            }

            solutions.add(solution);

            R currentResult = solution.getResult();
            R merged = merger.merge(lastResult, currentResult);

            lastResult = Optional.of(merged);
            if (evaluator.isDone(merged)) {
                break;
            }
        }

        debugPath(solutions);
        partitionComparison.analyzeSolutions(solutions, solvableFactory.getQueryKey());

        R result = merger.done(lastResult, defaultValue);

        log.inc("callAndMerge>all");
        log.inc("callAndMerge>tenant>" + tenantId);
        log.inc("callAndMerge>query>" + solvableFactory.getQueryKey());
        log.inc("callAndMerge>tenantAndQuery>" + tenantId + '>' + solvableFactory.getQueryKey());

        return result;
    }

    @Override
    public <R, P> R callImmediate(
            MiruTenantId tenantId,
            MiruPartitionId partitionId,
            MiruSolvableFactory<R, P> solvableFactory,
            Optional<R> lastResult,
            R defaultValue) throws Exception {
        Optional<MiruHostedPartition<?>> partition = getLocalTenantPartition(tenantId, partitionId);

        if (partition.isPresent()) {
            Callable<R> callable = solvableFactory.create(partition.get(), lastResult);
            R result = callable.call();

            log.inc("callImmediate>all");
            log.inc("callImmediate>tenant>" + tenantId);
            log.inc("callImmediate>query>" + solvableFactory.getQueryKey());
            log.inc("callImmediate>tenantAndQuery>" + tenantId + '>' + solvableFactory.getQueryKey());

            return result;
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
        try (MiruQueryHandle<BM> handle = partition.getQueryHandle()) {
            MiruQueryStream<BM> queryStream = handle.getQueryStream();
            int fieldId = queryStream.schema.getFieldId(fieldName);
            MiruField<BM> field = queryStream.fieldIndex.getField(fieldId);
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

    private <R> void debugPath(List<MiruSolution<R>> solutions) {
        if (log.isDebugEnabled()) {
            StringBuilder buf = new StringBuilder();
            int i = 0;
            for (MiruSolution<R> solution : solutions) {
                buf.append("\n  ").append(++i).append(". ").append(solution.getCoord()).append(" = ").append(solution.getResult());
            }

            log.debug("Partition path from {}:{}", localhost, buf);
        }
    }

}
