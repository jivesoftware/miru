package com.jivesoftware.os.miru.service.stream;

import com.google.common.base.Optional;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.hash.Hashing;
import com.jivesoftware.os.jive.utils.base.util.locks.StripingLocksProvider;
import com.jivesoftware.os.jive.utils.logger.MetricLogger;
import com.jivesoftware.os.jive.utils.logger.MetricLoggerFactory;
import com.jivesoftware.os.miru.api.activity.MiruActivity;
import com.jivesoftware.os.miru.api.activity.schema.MiruFieldDefinition;
import com.jivesoftware.os.miru.api.base.MiruTermId;
import com.jivesoftware.os.miru.plugin.bitmap.MiruBitmaps;
import com.jivesoftware.os.miru.plugin.index.BloomIndex;
import com.jivesoftware.os.miru.plugin.index.MiruActivityAndId;
import com.jivesoftware.os.miru.plugin.index.MiruIndexUtil;
import com.jivesoftware.os.miru.plugin.index.MiruInternalActivity;
import com.jivesoftware.os.miru.plugin.index.MiruInvertedIndex;
import gnu.trove.list.TIntList;
import gnu.trove.list.array.TIntArrayList;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

/**
 * Handles indexing of activity, including repair and removal, with synchronization and attention to versioning.
 */
public class MiruIndexer<BM> {

    private final static MetricLogger log = MetricLoggerFactory.getLogger();

    private final BloomIndex<BM> bloomIndex;
    private final MiruIndexUtil indexUtil = new MiruIndexUtil();
    private final MiruTermId fieldAggregateTermId = indexUtil.makeFieldAggregate();
    private final StripingLocksProvider<Integer> stripingLocksProvider = new StripingLocksProvider<>(64); // TODO expose to config

    public MiruIndexer(MiruBitmaps<BM> bitmaps) {
        this.bloomIndex = new BloomIndex<>(bitmaps, Hashing.murmur3_128(), 100_000, 0.01f); // TODO fix somehow
    }

    public void index(final MiruContext<BM> context, final List<MiruActivityAndId<MiruActivity>> activityAndIds, ExecutorService indexExecutor)
        throws Exception {

        @SuppressWarnings("unchecked")
        final List<MiruActivityAndId<MiruInternalActivity>> internalActivityAndIds = Arrays.<MiruActivityAndId<MiruInternalActivity>>asList(
            new MiruActivityAndId[activityAndIds.size()]);

        log.debug("Start: Index batch of {}", activityAndIds.size());

        final int numPartitions = 48;
        final int numActivities = internalActivityAndIds.size();
        final int partitionSize = (numActivities + numPartitions - 1) / numPartitions;

        List<Future<?>> internFutures = new ArrayList<>(numPartitions);
        for (int i = 0; i < activityAndIds.size(); i += partitionSize) {
            final int startOfSubList = i;
            internFutures.add(indexExecutor.submit(new Callable<Void>() {
                @Override
                public Void call() throws Exception {
                    context.activityInternExtern.intern(activityAndIds, startOfSubList, partitionSize, internalActivityAndIds, context.schema);
                    return null;
                }
            }));
        }
        awaitInternFutures(internFutures);
        activityAndIds.clear();  // This frees up the MiruActivityAndId<MiruActivity> to be garbage collected.

        List<Future<List<FieldValuesWork>>> fieldWorkFutures = composeFieldValuesWork(context, internalActivityAndIds, indexExecutor);
        final List<Future<List<BloomWork>>> bloominsWorkFutures = composeBloominsWork(context, internalActivityAndIds, indexExecutor);
        final List<Future<List<AggregateFieldsWork>>> aggregateFieldsWorkFutures = composeAggregateFieldsWork(context, internalActivityAndIds, indexExecutor);

        List<FieldValuesWork>[] fieldsWork = awaitFieldWorkFutures(fieldWorkFutures);

        List<Future<?>> fieldFutures = indexFieldValues(context, fieldsWork, indexExecutor);

        Future<List<BloomWork>> bloominsSortFuture = indexExecutor.submit(new Callable<List<BloomWork>>() {
            @Override
            public List<BloomWork> call() throws Exception {
                List<BloomWork> bloominsWork = Lists.newArrayList();
                for (Future<List<BloomWork>> future : bloominsWorkFutures) {
                    bloominsWork.addAll(future.get());
                }
                Collections.sort(bloominsWork);
                return bloominsWork;
            }
        });

        Future<List<AggregateFieldsWork>> aggregateFieldsSortFuture = indexExecutor.submit(new Callable<List<AggregateFieldsWork>>() {
            @Override
            public List<AggregateFieldsWork> call() throws Exception {
                List<AggregateFieldsWork> aggregateFieldsWork = Lists.newArrayList();
                for (Future<List<AggregateFieldsWork>> future : aggregateFieldsWorkFutures) {
                    aggregateFieldsWork.addAll(future.get());
                }
                Collections.sort(aggregateFieldsWork);
                return aggregateFieldsWork;
            }
        });

        awaitFieldFutures(fieldFutures);

        List<BloomWork> bloominsWork = awaitBloominsSortFuture(bloominsSortFuture);
        List<AggregateFieldsWork> aggregateFieldsWork = awaitAggregateFieldsSortFuture(aggregateFieldsSortFuture);

        final List<Future<?>> otherFutures = new ArrayList<>();
        otherFutures.add(indexExecutor.submit(new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                indexAuthz(context, internalActivityAndIds);
                return null;
            }
        }));
        otherFutures.addAll(indexAggregateFields(context, aggregateFieldsWork, indexExecutor));
        otherFutures.addAll(indexBloomins(context, bloominsWork, indexExecutor));
        otherFutures.addAll(indexWriteTimeAggregates(context, internalActivityAndIds, indexExecutor));
        for (final List<MiruActivityAndId<MiruInternalActivity>> partition : Lists.partition(internalActivityAndIds, partitionSize)) {
            otherFutures.add(indexExecutor.submit(new Callable<Void>() {
                @Override
                public Void call() throws Exception {
                    context.activityIndex.set(partition);
                    return null;
                }
            }));
        }
        awaitOtherFutures(otherFutures);

        if (!internalActivityAndIds.isEmpty()) {
            context.activityIndex.ready(internalActivityAndIds.get(internalActivityAndIds.size() - 1).id);
        }

        log.debug("End: Index batch of {}", internalActivityAndIds.size());
    }

    public void set(MiruContext<BM> context, List<MiruActivityAndId<MiruActivity>> activityAndIds) throws Exception {
        @SuppressWarnings("unchecked")
        List<MiruActivityAndId<MiruInternalActivity>> internalActivityAndIds = Arrays.<MiruActivityAndId<MiruInternalActivity>>asList(
            new MiruActivityAndId[activityAndIds.size()]);

        context.activityInternExtern.intern(activityAndIds, 0, activityAndIds.size(), internalActivityAndIds, context.schema);
        context.activityIndex.setAndReady(internalActivityAndIds);
    }

    public void repair(MiruContext<BM> context, MiruActivity activity, int id) throws Exception {
        synchronized (stripingLocksProvider.lock(id)) {
            MiruInternalActivity existing = context.activityIndex.get(activity.tenantId, id);
            if (existing == null) {
                log.debug("Can't repair nonexistent activity at {}\n- offered: {}", id, activity);
            } else if (activity.version <= existing.version) {
                log.debug("Declined to repair old activity at {}\n- have: {}\n- offered: {}", id, existing, activity);
            } else {
                log.debug("Repairing activity at {}\n- was: {}\n- now: {}", id, existing, activity);
                @SuppressWarnings("unchecked")
                List<MiruActivityAndId<MiruInternalActivity>> internalActivityAndIds = Arrays.<MiruActivityAndId<MiruInternalActivity>>asList(
                    new MiruActivityAndId[1]);
                context.activityInternExtern.intern(Arrays.asList(new MiruActivityAndId<>(activity, id)), 0, 1, internalActivityAndIds, context.schema);

                for (MiruActivityAndId<MiruInternalActivity> internalActivityAndId : internalActivityAndIds) {
                    MiruInternalActivity internalActivity = internalActivityAndId.activity;
                    Set<String> existingAuthz = existing.authz != null ? Sets.newHashSet(existing.authz) : Sets.<String>newHashSet();
                    Set<String> repairedAuthz = internalActivity.authz != null ? Sets.newHashSet(internalActivity.authz) : Sets.<String>newHashSet();

                    for (String authz : existingAuthz) {
                        if (!repairedAuthz.contains(authz)) {
                            repairAuthz(context, authz, id, false);
                        }
                    }

                    for (String authz : repairedAuthz) {
                        if (!existingAuthz.contains(authz)) {
                            repairAuthz(context, authz, id, true);
                        }
                    }
                }

                //TODO repair fields?
                // repairs also unhide (remove from removal)
                context.removalIndex.remove(id);

                // finally, update the activity index
                context.activityIndex.setAndReady(internalActivityAndIds);
            }
        }
    }

    public void remove(MiruContext<BM> context, MiruActivity activity, int id) throws Exception {
        synchronized (stripingLocksProvider.lock(id)) {
            MiruInternalActivity existing = context.activityIndex.get(activity.tenantId, id);
            if (existing == null) {
                log.debug("Can't remove nonexistent activity at {}\n- offered: {}", id, activity);
            } else if (activity.version <= existing.version) {
                log.debug("Declined to remove old activity at {}\n- have: {}\n- offered: {}", id, existing, activity);
            } else {
                log.debug("Removing activity at {}\n- was: {}\n- now: {}", id, existing, activity);
                @SuppressWarnings("unchecked")
                List<MiruActivityAndId<MiruInternalActivity>> internalActivity = Arrays.<MiruActivityAndId<MiruInternalActivity>>asList(
                    new MiruActivityAndId[1]);
                context.activityInternExtern.intern(Arrays.asList(new MiruActivityAndId<>(activity, id)), 0, 1, internalActivity, context.schema);

                //TODO apply field changes?
                // hide (add to removal)
                context.removalIndex.set(id);

                // finally, update the activity index
                context.activityIndex.setAndReady(internalActivity);
            }
        }
    }

    private void awaitInternFutures(List<Future<?>> internFutures) throws InterruptedException, ExecutionException {
        awaitFutures(internFutures, "Intern");
    }

    private List<FieldValuesWork>[] awaitFieldWorkFutures(List<Future<List<FieldValuesWork>>> fieldWorkFutures)
        throws InterruptedException, ExecutionException {

        @SuppressWarnings("unchecked")
        List<FieldValuesWork>[] fieldsWork = new List[fieldWorkFutures.size()];
        for (int i = 0; i < fieldWorkFutures.size(); i++) {
            fieldsWork[i] = fieldWorkFutures.get(i).get();
            Collections.sort(fieldsWork[i]);
        }
        return fieldsWork;
    }

    private void awaitFieldFutures(List<Future<?>> fieldFutures) throws InterruptedException, ExecutionException {
        awaitFutures(fieldFutures, "IndexFields");
    }

    private List<BloomWork> awaitBloominsSortFuture(Future<List<BloomWork>> bloominsSortFuture) throws InterruptedException, ExecutionException {
        return bloominsSortFuture.get();
    }

    private List<AggregateFieldsWork> awaitAggregateFieldsSortFuture(Future<List<AggregateFieldsWork>> aggregateFieldsSortFuture) throws
        InterruptedException, ExecutionException {
        return aggregateFieldsSortFuture.get();
    }

    private void awaitOtherFutures(List<Future<?>> otherFutures) throws InterruptedException, ExecutionException {
        awaitFutures(otherFutures, "IndexOthers");
    }

    private void awaitFutures(List<Future<?>> futures, String futureName) throws InterruptedException, ExecutionException {
        long start = System.currentTimeMillis();
        for (Future<?> future : futures) {
            future.get();
        }
        if (log.isTraceEnabled()) {
            log.trace(futureName + ": Finished waiting for futures in " + (System.currentTimeMillis() - start) + " ms");
        }
    }

    private List<Future<List<FieldValuesWork>>> composeFieldValuesWork(MiruContext<BM> context,
        final List<MiruActivityAndId<MiruInternalActivity>> internalActivityAndIds,
        ExecutorService indexExecutor)
        throws Exception {

        List<Integer> fieldIds = context.schema.getFieldIds();
        List<Future<List<FieldValuesWork>>> workFutures = new ArrayList<>(fieldIds.size());
        for (final int fieldId : fieldIds) {
            workFutures.add(indexExecutor.submit(new Callable<List<FieldValuesWork>>() {
                @Override
                public List<FieldValuesWork> call() throws Exception {
                    Map<MiruTermId, TIntList> fieldWork = Maps.newHashMap();
                    for (MiruActivityAndId<MiruInternalActivity> internalActivityAndId : internalActivityAndIds) {
                        MiruInternalActivity activity = internalActivityAndId.activity;
                        if (activity.fieldsValues[fieldId] != null) {
                            for (MiruTermId term : activity.fieldsValues[fieldId]) {
                                TIntList ids = fieldWork.get(term);
                                if (ids == null) {
                                    ids = new TIntArrayList();
                                    fieldWork.put(term, ids);
                                }
                                ids.add(internalActivityAndId.id);
                            }
                        }
                    }
                    List<FieldValuesWork> workList = Lists.newArrayListWithCapacity(fieldWork.size());
                    for (Map.Entry<MiruTermId, TIntList> entry : fieldWork.entrySet()) {
                        workList.add(new FieldValuesWork(entry.getKey(), entry.getValue()));
                    }
                    return workList;
                }
            }));
        }
        return workFutures;
    }

    private List<Future<?>> indexFieldValues(final MiruContext<BM> context, final List<FieldValuesWork>[] work, ExecutorService indexExecutor)
        throws Exception {
        List<Integer> fieldIds = context.schema.getFieldIds();
        List<Future<?>> futures = new ArrayList<>(fieldIds.size());
        for (int fieldId = 0; fieldId < work.length; fieldId++) {
            List<FieldValuesWork> fieldWork = work[fieldId];
            final int finalFieldId = fieldId;
            for (final FieldValuesWork fieldValuesWork : fieldWork) {
                futures.add(indexExecutor.submit(new Callable<Void>() {
                    @Override
                    public Void call() throws Exception {
                        context.fieldIndex.index(finalFieldId, fieldValuesWork.fieldValue, fieldValuesWork.ids.toArray());
                        return null;
                    }
                }));
            }
        }
        return futures;
    }

    private void indexAuthz(MiruContext<BM> context, List<MiruActivityAndId<MiruInternalActivity>> internalActivityAndIds) throws Exception {
        // TODO rewrite to use same pattern as indexFieldValues
        for (MiruActivityAndId<MiruInternalActivity> internalActivityAndId : internalActivityAndIds) {
            MiruInternalActivity activity = internalActivityAndId.activity;
            if (activity.authz != null) {
                for (String authz : activity.authz) {
                    context.authzIndex.index(authz, internalActivityAndId.id);
                }
            }
        }
    }

    private void repairAuthz(MiruContext<BM> context, String authz, int id, boolean value) throws Exception {
        context.authzIndex.repair(authz, id, value);
    }

    private List<Future<List<BloomWork>>> composeBloominsWork(MiruContext<BM> context,
        final List<MiruActivityAndId<MiruInternalActivity>> internalActivityAndIds,
        ExecutorService indexExecutor)
        throws Exception {

        List<MiruFieldDefinition> fieldsWithBlooms = context.schema.getFieldsWithBlooms();
        List<Future<List<BloomWork>>> workFutures = Lists.newArrayList();
        for (final MiruFieldDefinition fieldDefinition : fieldsWithBlooms) {
            List<MiruFieldDefinition> bloominFieldDefinitions = context.schema.getBloominFieldDefinitions(fieldDefinition.fieldId);
            for (final MiruFieldDefinition bloominFieldDefinition : bloominFieldDefinitions) {
                workFutures.add(indexExecutor.submit(new Callable<List<BloomWork>>() {
                    @Override
                    public List<BloomWork> call() throws Exception {
                        Map<MiruTermId, List<MiruTermId>> fieldValueWork = Maps.newHashMap();
                        for (MiruActivityAndId<MiruInternalActivity> internalActivityAndId : internalActivityAndIds) {
                            MiruInternalActivity activity = internalActivityAndId.activity;
                            MiruTermId[] fieldValues = activity.fieldsValues[fieldDefinition.fieldId];
                            if (fieldValues != null && fieldValues.length > 0) {
                                final MiruTermId[] bloomFieldValues = activity.fieldsValues[bloominFieldDefinition.fieldId];
                                if (bloomFieldValues != null && bloomFieldValues.length > 0) {
                                    for (final MiruTermId fieldValue : fieldValues) {
                                        List<MiruTermId> combinedBloomFieldValues = fieldValueWork.get(fieldValue);
                                        if (combinedBloomFieldValues == null) {
                                            combinedBloomFieldValues = Lists.newArrayList();
                                            fieldValueWork.put(fieldValue, combinedBloomFieldValues);
                                        }
                                        Collections.addAll(combinedBloomFieldValues, bloomFieldValues);
                                    }
                                }
                            }
                        }
                        List<BloomWork> workList = Lists.newArrayListWithCapacity(fieldValueWork.size());
                        for (Map.Entry<MiruTermId, List<MiruTermId>> entry : fieldValueWork.entrySet()) {
                            workList.add(new BloomWork(fieldDefinition.fieldId, bloominFieldDefinition.fieldId, entry.getKey(), entry.getValue()));
                        }
                        return workList;
                    }
                }));
            }
        }
        return workFutures;
    }

    private List<Future<?>> indexBloomins(final MiruContext<BM> context, List<BloomWork> bloomWorks, ExecutorService indexExecutor) {
        int callableCount = 0;
        List<Future<?>> futures = Lists.newArrayList();
        for (final BloomWork bloomWork : bloomWorks) {
            futures.add(indexExecutor.submit(new Callable<Void>() {
                @Override
                public Void call() throws Exception {
                    MiruFieldDefinition bloomFieldDefinition = context.schema.getFieldDefinition(bloomWork.bloomFieldId);
                    MiruTermId compositeBloomId = indexUtil.makeBloomComposite(bloomWork.fieldValue, bloomFieldDefinition.name);
                    MiruInvertedIndex<BM> invertedIndex = context.fieldIndex.getOrCreateInvertedIndex(bloomWork.fieldId, compositeBloomId);
                    bloomIndex.put(invertedIndex, bloomWork.bloomFieldValues);
                    return null;
                }
            }));
            callableCount++;
        }
        log.trace("Submitted {} bloom callables", callableCount);

        return futures;
    }

    private List<Future<?>> indexWriteTimeAggregates(final MiruContext<BM> context,
        List<MiruActivityAndId<MiruInternalActivity>> internalActivityAndIds,
        ExecutorService indexExecutor)
        throws Exception {

        List<MiruFieldDefinition> writeTimeAggregateFields = context.schema.getWriteTimeAggregateFields();
        // rough estimate of necessary capacity
        List<Future<?>> futures = Lists.newArrayListWithCapacity(internalActivityAndIds.size() * writeTimeAggregateFields.size());
        for (final MiruActivityAndId<MiruInternalActivity> internalActivityAndId : internalActivityAndIds) {
            for (final MiruFieldDefinition fieldDefinition : writeTimeAggregateFields) {
                final MiruTermId[] fieldValues = internalActivityAndId.activity.fieldsValues[fieldDefinition.fieldId];
                if (fieldValues != null && fieldValues.length > 0) {
                    futures.add(indexExecutor.submit(new Callable<Void>() {
                        @Override
                        public Void call() throws Exception {
                            // Answers the question,
                            // "What is the latest activity against each distinct value of this field?"
                            MiruInvertedIndex<BM> aggregateIndex = context.fieldIndex.getOrCreateInvertedIndex(fieldDefinition.fieldId, fieldAggregateTermId);

                            // ["doc"] -> "d1", "d2", "d3", "d4" -> [0, 1(d1), 0, 0, 1(d2), 0, 0, 1(d3), 0, 0, 1(d4)]
                            for (MiruTermId fieldValue : fieldValues) {
                                Optional<MiruInvertedIndex<BM>> optionalFieldValueIndex = context.fieldIndex.get(fieldDefinition.fieldId, fieldValue);
                                if (optionalFieldValueIndex.isPresent()) {
                                    MiruInvertedIndex<BM> fieldValueIndex = optionalFieldValueIndex.get();
                                    aggregateIndex.andNotToSourceSize(Collections.singletonList(fieldValueIndex.getIndexUnsafe()));
                                }
                            }
                            context.fieldIndex.index(fieldDefinition.fieldId, fieldAggregateTermId, internalActivityAndId.id);
                            return null;
                        }
                    }));
                }
            }
        }
        return futures;
    }

    // Answers the question,
    // "For each distinct value of this field, what is the latest activity against each distinct value of the related field?"
    private List<Future<List<AggregateFieldsWork>>> composeAggregateFieldsWork(MiruContext<BM> context,
        final List<MiruActivityAndId<MiruInternalActivity>> internalActivityAndIds,
        ExecutorService indexExecutor)
        throws Exception {

        List<MiruFieldDefinition> fieldsWithAggregates = context.schema.getFieldsWithAggregates();
        List<Future<List<AggregateFieldsWork>>> workFutures = Lists.newArrayList();
        for (final MiruFieldDefinition fieldDefinition : fieldsWithAggregates) {
            List<MiruFieldDefinition> aggregateFieldDefinitions = context.schema.getAggregateFieldDefinitions(fieldDefinition.fieldId);
            for (final MiruFieldDefinition aggregateFieldDefinition : aggregateFieldDefinitions) {
                workFutures.add(indexExecutor.submit(new Callable<List<AggregateFieldsWork>>() {
                    @Override
                    public List<AggregateFieldsWork> call() throws Exception {
                        Map<MiruTermId, List<IdAndTerm>> fieldWork = Maps.newHashMap();
                        Set<WriteAggregateKey> visited = Sets.newHashSet();

                        // walk backwards so we see the largest id first, and mark visitors for each coordinate
                        for (int i = internalActivityAndIds.size() - 1; i >= 0; i--) {
                            final MiruActivityAndId<MiruInternalActivity> internalActivityAndId = internalActivityAndIds.get(i);
                            MiruInternalActivity activity = internalActivityAndId.activity;
                            MiruTermId[] fieldValues = activity.fieldsValues[fieldDefinition.fieldId];
                            if (fieldValues != null && fieldValues.length > 0) {
                                MiruTermId[] aggregateFieldValues = activity.fieldsValues[aggregateFieldDefinition.fieldId];
                                if (aggregateFieldValues != null && aggregateFieldValues.length > 0) {
                                    for (final MiruTermId fieldValue : fieldValues) {
                                        for (MiruTermId aggregateFieldValue : aggregateFieldValues) {
                                            WriteAggregateKey key = new WriteAggregateKey(fieldValue, aggregateFieldValue);
                                            if (visited.add(key)) {
                                                List<IdAndTerm> idAndTerms = fieldWork.get(fieldValue);
                                                if (idAndTerms == null) {
                                                    idAndTerms = Lists.newArrayList();
                                                    fieldWork.put(fieldValue, idAndTerms);
                                                }
                                                idAndTerms.add(new IdAndTerm(internalActivityAndId.id, aggregateFieldValue));
                                            }
                                        }
                                    }
                                }
                            }
                        }
                        List<AggregateFieldsWork> workList = Lists.newArrayListWithCapacity(fieldWork.size());
                        for (Map.Entry<MiruTermId, List<IdAndTerm>> entry : fieldWork.entrySet()) {
                            workList.add(new AggregateFieldsWork(fieldDefinition.fieldId, aggregateFieldDefinition.fieldId, entry.getKey(), entry.getValue()));
                        }
                        return workList;
                    }
                }));
            }
        }
        return workFutures;
    }

    private List<Future<?>> indexAggregateFields(final MiruContext<BM> context,
        List<AggregateFieldsWork> aggregateFieldsWorks,
        ExecutorService indexExecutor)
        throws Exception {

        int callableCount = 0;
        List<Future<?>> futures = Lists.newArrayListWithCapacity(aggregateFieldsWorks.size());
        for (final AggregateFieldsWork aggregateFieldsWork : aggregateFieldsWorks) {
            futures.add(indexExecutor.submit(new Callable<Void>() {
                @Override
                public Void call() throws Exception {
                    MiruTermId fieldValue = aggregateFieldsWork.fieldValue;
                    List<IdAndTerm> idAndTerms = aggregateFieldsWork.work;

                    MiruFieldDefinition aggregateFieldDefinition = context.schema.getFieldDefinition(aggregateFieldsWork.aggregateFieldId);
                    MiruTermId compositeAggregateId = indexUtil.makeFieldValueAggregate(fieldValue, aggregateFieldDefinition.name);
                    MiruInvertedIndex<BM> invertedIndex = context.fieldIndex.getOrCreateInvertedIndex(aggregateFieldsWork.fieldId, compositeAggregateId);

                    List<BM> aggregateBitmaps = Lists.newArrayListWithCapacity(idAndTerms.size());
                    TIntList ids = new TIntArrayList(idAndTerms.size());
                    for (IdAndTerm idAndTerm : idAndTerms) {
                        MiruTermId aggregateFieldValue = idAndTerm.term;
                        MiruInvertedIndex<BM> aggregateInvertedIndex = context.fieldIndex.getOrCreateInvertedIndex(
                            aggregateFieldsWork.aggregateFieldId, aggregateFieldValue);
                        BM aggregateBitmap = aggregateInvertedIndex.getIndexUnsafe();
                        aggregateBitmaps.add(aggregateBitmap);
                        ids.add(idAndTerm.id);
                    }

                    invertedIndex.andNotToSourceSize(aggregateBitmaps);

                    ids.reverse(); // we built in reverse order, so flip back to ascending
                    context.fieldIndex.index(aggregateFieldsWork.fieldId, compositeAggregateId, ids.toArray());

                    return null;
                }
            }));
            callableCount++;
        }

        log.trace("Submitted {} aggregate field callables", callableCount);

        return futures;
    }

}
