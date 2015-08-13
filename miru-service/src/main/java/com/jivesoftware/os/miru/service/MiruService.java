package com.jivesoftware.os.miru.service;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Iterables;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Lists;
import com.jivesoftware.os.miru.api.MiruBackingStorage;
import com.jivesoftware.os.miru.api.MiruHost;
import com.jivesoftware.os.miru.api.MiruPartitionCoord;
import com.jivesoftware.os.miru.api.MiruPartitionCoordInfo;
import com.jivesoftware.os.miru.api.activity.CoordinateStream;
import com.jivesoftware.os.miru.api.activity.MiruPartitionId;
import com.jivesoftware.os.miru.api.activity.MiruPartitionedActivity;
import com.jivesoftware.os.miru.api.activity.schema.MiruFieldDefinition;
import com.jivesoftware.os.miru.api.activity.schema.MiruSchemaProvider;
import com.jivesoftware.os.miru.api.activity.schema.MiruSchemaUnvailableException;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.api.field.MiruFieldType;
import com.jivesoftware.os.miru.api.wal.MiruSipCursor;
import com.jivesoftware.os.miru.plugin.Miru;
import com.jivesoftware.os.miru.plugin.bitmap.MiruBitmapsDebug;
import com.jivesoftware.os.miru.plugin.context.MiruRequestContext;
import com.jivesoftware.os.miru.plugin.context.RequestContextCallback;
import com.jivesoftware.os.miru.plugin.partition.MiruPartitionDirector;
import com.jivesoftware.os.miru.plugin.partition.MiruPartitionUnavailableException;
import com.jivesoftware.os.miru.plugin.partition.MiruQueryablePartition;
import com.jivesoftware.os.miru.plugin.partition.OrderedPartitions;
import com.jivesoftware.os.miru.plugin.solution.MiruAnswerEvaluator;
import com.jivesoftware.os.miru.plugin.solution.MiruAnswerMerger;
import com.jivesoftware.os.miru.plugin.solution.MiruPartitionResponse;
import com.jivesoftware.os.miru.plugin.solution.MiruRequestHandle;
import com.jivesoftware.os.miru.plugin.solution.MiruResponse;
import com.jivesoftware.os.miru.plugin.solution.MiruSolution;
import com.jivesoftware.os.miru.plugin.solution.MiruSolutionLog;
import com.jivesoftware.os.miru.plugin.solution.MiruSolutionLogLevel;
import com.jivesoftware.os.miru.plugin.solution.MiruSolvable;
import com.jivesoftware.os.miru.plugin.solution.MiruSolvableFactory;
import com.jivesoftware.os.miru.service.partition.MiruHostedPartitionComparison;
import com.jivesoftware.os.miru.service.solver.MiruSolved;
import com.jivesoftware.os.miru.service.solver.MiruSolver;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import java.util.Collections;
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
    private final MiruSchemaProvider schemaProvider;
    private final MiruBitmapsDebug bitmapsDebug = new MiruBitmapsDebug();

    public MiruService(MiruHost localhost,
        MiruPartitionDirector partitionDirector,
        MiruHostedPartitionComparison partitionComparison,
        MiruSolver solver,
        MiruSchemaProvider schemaProvider) {

        this.localhost = localhost;
        this.partitionDirector = partitionDirector;
        this.partitionComparison = partitionComparison;
        this.solver = solver;
        this.schemaProvider = schemaProvider;
    }

    public void writeToIndex(List<MiruPartitionedActivity> partitionedActivities) throws Exception {
        ListMultimap<MiruTenantId, MiruPartitionedActivity> perTenantPartitionedActivities = ArrayListMultimap.create();
        for (MiruPartitionedActivity partitionedActivity : partitionedActivities) {
            perTenantPartitionedActivities.put(partitionedActivity.tenantId, partitionedActivity);
        }
        partitionDirector.index(perTenantPartitionedActivities);
    }

    @Override
    public <Q, A, P> MiruResponse<A> askAndMerge(
        MiruTenantId tenantId,
        final MiruSolvableFactory<Q, A, P> solvableFactory,
        MiruAnswerEvaluator<A> evaluator,
        MiruAnswerMerger<A> merger,
        A defaultValue,
        MiruSolutionLogLevel logLevel)
        throws Exception {

        try {
            schemaProvider.getSchema(tenantId);
        } catch (MiruSchemaUnvailableException e) {
            return new MiruResponse<>(null, null, 0, true, Collections.<Integer>emptyList(),
                Collections.singletonList("Schema has not been registered for this tenantId"));
        }

        log.startTimer("askAndMerge");

        A answer = null;
        List<MiruSolution> solutions = Lists.newArrayList();
        List<Integer> incompletePartitionIds = Lists.newArrayList();
        final MiruSolutionLog solutionLog = new MiruSolutionLog(logLevel);
        solutionLog.log(MiruSolutionLogLevel.INFO, "Solving: host:{} tenantId:{} question:{}", localhost, tenantId, solvableFactory.getQuestion());
        long totalElapsed;

        try {
            Iterable<? extends OrderedPartitions<?>> partitionReplicas = partitionDirector.allQueryablePartitionsInOrder(
                tenantId, solvableFactory.getQueryKey());

            Optional<A> lastAnswer = Optional.absent();

            for (OrderedPartitions<?> orderedPartitions : partitionReplicas) {

                final Optional<A> optionalAnswer = lastAnswer;
                Iterable<MiruSolvable<A>> solvables = Iterables.transform(orderedPartitions.partitions,
                    new Function<MiruQueryablePartition<?>, MiruSolvable<A>>() {
                        @Override
                        public MiruSolvable<A> apply(final MiruQueryablePartition<?> replica) {
                            if (replica.isLocal()) {
                                solutionLog.log(MiruSolutionLogLevel.INFO, "Created local solvable for coord={}.", replica.getCoord());
                            }
                            return solvableFactory.create(replica, solvableFactory.getReport(optionalAnswer));
                        }
                    });

                Optional<Long> suggestedTimeoutInMillis = partitionComparison.suggestTimeout(orderedPartitions.tenantId, orderedPartitions.partitionId,
                    solvableFactory.getQueryKey());
                solutionLog.log(MiruSolutionLogLevel.INFO, "Solving partition:{}", orderedPartitions.partitionId.getId()
                    + " for tenant:" + orderedPartitions.tenantId
                    + " timeout:" + suggestedTimeoutInMillis.or(-1L));

                long start = System.currentTimeMillis();
                MiruSolved<A> solved = solver.solve(solvables.iterator(), suggestedTimeoutInMillis, solutionLog);
                if (solved == null) {
                    solutionLog.log(MiruSolutionLogLevel.WARN, "No solution for partition:{}", orderedPartitions.partitionId);
                    solutionLog.log(MiruSolutionLogLevel.WARN, "WARNING result set is incomplete! elapse:{}", (System.currentTimeMillis() - start));
                    incompletePartitionIds.add(orderedPartitions.partitionId.getId());
                    if (evaluator.stopOnUnsolvablePartition()) {
                        solutionLog.log(MiruSolutionLogLevel.ERROR, "ERROR result set is unsolvable!");
                        break;
                    }
                } else {
                    solutionLog.log(MiruSolutionLogLevel.INFO, "Solved partition:{}. elapse:{} millis",
                        orderedPartitions.partitionId, (System.currentTimeMillis() - start));
                    solutions.add(solved.solution);

                    A currentAnswer = solved.answer;
                    solutionLog.log(MiruSolutionLogLevel.INFO, "Merging solution set from partition:{}", orderedPartitions.partitionId);
                    start = System.currentTimeMillis();
                    A merged = merger.merge(lastAnswer, currentAnswer, solutionLog);
                    solutionLog.log(MiruSolutionLogLevel.INFO, "Merged. elapse:{} millis", (System.currentTimeMillis() - start));

                    lastAnswer = Optional.of(merged);
                    if (evaluator.isDone(merged, solutionLog)) {
                        break;
                    }
                }
            }

            partitionComparison.analyzeSolutions(solutions, solvableFactory.getQueryKey());

            answer = merger.done(lastAnswer, defaultValue, solutionLog);

        } finally {
            totalElapsed = log.stopTimer("askAndMerge");
        }

        log.inc("askAndMerge");
        log.inc("askAndMerge", tenantId.toString());
        log.inc("askAndMerge>query>" + solvableFactory.getQueryKey());
        log.inc("askAndMerge>query>" + solvableFactory.getQueryKey(), tenantId.toString());

        return new MiruResponse<>(answer, solutions, totalElapsed, false, incompletePartitionIds, solutionLog.asList());
    }

    @Override
    public <Q, A, P> MiruPartitionResponse<A> askImmediate(
        MiruTenantId tenantId,
        MiruPartitionId partitionId,
        MiruSolvableFactory<Q, A, P> factory,
        Optional<P> report,
        A defaultValue,
        MiruSolutionLogLevel logLevel)
        throws Exception {
        Optional<? extends MiruQueryablePartition<?>> partition = getLocalTenantPartition(tenantId, partitionId);

        if (partition.isPresent()) {
            Callable<MiruPartitionResponse<A>> callable = factory.create(partition.get(), report);
            MiruPartitionResponse<A> answer = callable.call();

            log.inc("askImmediate");
            log.inc("askImmediate", tenantId.toString());
            log.inc("askImmediate>query" + factory.getQueryKey());
            log.inc("askImmediate>query" + factory.getQueryKey(), tenantId.toString());

            return answer;
        } else {
            throw new MiruPartitionUnavailableException("partition is NOT present. partitionId:" + partitionId);
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
        Optional<? extends MiruQueryablePartition<?>> partition = getLocalTenantPartition(tenantId, partitionId);
        if (partition.isPresent()) {
            return inspect(partition.get(), fieldName, termValue);
        } else {
            return "Partition unavailable";
        }
    }

    private <BM> String inspect(MiruQueryablePartition<BM> partition, String fieldName, String termValue) throws Exception {
        try (MiruRequestHandle<BM, ?> handle = partition.acquireQueryHandle()) {
            MiruRequestContext<BM, ?> requestContext = handle.getRequestContext();
            int fieldId = requestContext.getSchema().getFieldId(fieldName);
            MiruFieldDefinition fieldDefinition = requestContext.getSchema().getFieldDefinition(fieldId);
            Optional<BM> index = requestContext.getFieldIndexProvider()
                .getFieldIndex(MiruFieldType.primary)
                .get(fieldId, requestContext.getTermComposer().compose(fieldDefinition, termValue))
                .getIndex();
            if (index.isPresent()) {
                return bitmapsDebug.toString(handle.getBitmaps(), index.get());
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

    public void removeTopology(MiruTenantId tenantId, MiruPartitionId partitionId, MiruHost host) throws Exception {
        partitionDirector.removeTopology(tenantId, partitionId, host);
    }

    public boolean checkInfo(MiruTenantId tenantId, MiruPartitionId partitionId, MiruPartitionCoordInfo info) throws Exception {
        return partitionDirector.checkInfo(tenantId, partitionId, info);
    }

    public boolean prioritizeRebuild(MiruTenantId tenantId, MiruPartitionId partitionId) throws Exception {
        return partitionDirector.prioritizeRebuild(tenantId, partitionId);
    }

    private Optional<? extends MiruQueryablePartition<?>> getLocalTenantPartition(MiruTenantId tenantId, MiruPartitionId partitionId) throws Exception {
        MiruPartitionCoord localPartitionCoord = new MiruPartitionCoord(tenantId, partitionId, localhost);
        return partitionDirector.getQueryablePartition(localPartitionCoord);
    }

    public boolean expectedTopologies(CoordinateStream stream) throws Exception {
        return partitionDirector.expectedTopologies(stream);
    }

    public void introspect(MiruTenantId tenantId, MiruPartitionId partitionId, RequestContextCallback callback) throws Exception {
        Optional<? extends MiruQueryablePartition<?>> partition = getLocalTenantPartition(tenantId, partitionId);
        if (partition.isPresent()) {
            MiruQueryablePartition<?> hostedPartition = partition.get();
            try (MiruRequestHandle<?, ? extends MiruSipCursor<?>> handle = hostedPartition.tryQueryHandle()) {
                callback.call(handle.getRequestContext());
            }
        }
    }
}
