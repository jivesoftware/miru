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
import com.jivesoftware.os.miru.api.activity.schema.MiruSchema;
import com.jivesoftware.os.miru.api.base.MiruTermId;
import com.jivesoftware.os.miru.plugin.bitmap.MiruBitmaps;
import com.jivesoftware.os.miru.plugin.index.BloomIndex;
import com.jivesoftware.os.miru.plugin.index.MiruActivityAndId;
import com.jivesoftware.os.miru.plugin.index.MiruActivityIndex;
import com.jivesoftware.os.miru.plugin.index.MiruActivityInternExtern;
import com.jivesoftware.os.miru.plugin.index.MiruAuthzIndex;
import com.jivesoftware.os.miru.plugin.index.MiruField;
import com.jivesoftware.os.miru.plugin.index.MiruFields;
import com.jivesoftware.os.miru.plugin.index.MiruIndexUtil;
import com.jivesoftware.os.miru.plugin.index.MiruInternalActivity;
import com.jivesoftware.os.miru.plugin.index.MiruInvertedIndex;
import com.jivesoftware.os.miru.plugin.index.MiruRemovalIndex;
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
public class MiruIndexContext<BM> {

    private final static MetricLogger log = MetricLoggerFactory.getLogger();

    private final MiruBitmaps<BM> bitmaps;
    private final MiruSchema schema;
    private final MiruActivityIndex activityIndex;
    private final MiruFields<BM> fieldIndex;
    private final MiruAuthzIndex authzIndex;
    private final MiruRemovalIndex removalIndex;
    private final MiruActivityInternExtern activityInterner;
    private final ExecutorService indexExecutor;
    private final MiruIndexUtil indexUtil = new MiruIndexUtil();
    private final BloomIndex<BM> bloomIndex;

    private final MiruTermId fieldAggregateTermId = indexUtil.makeFieldAggregate();
    private final StripingLocksProvider<Integer> stripingLocksProvider = new StripingLocksProvider<>(64); // TODO expose to config

    public MiruIndexContext(MiruBitmaps<BM> bitmaps,
        MiruSchema schema,
        MiruActivityIndex activityIndex,
        MiruFields<BM> fieldIndex,
        MiruAuthzIndex authzIndex,
        MiruRemovalIndex removalIndex,
        MiruActivityInternExtern activityInterner,
        ExecutorService indexExecutor) {

        this.bitmaps = bitmaps;
        this.schema = schema;
        this.activityIndex = activityIndex;
        this.fieldIndex = fieldIndex;
        this.authzIndex = authzIndex;
        this.removalIndex = removalIndex;
        this.activityInterner = activityInterner;
        this.indexExecutor = indexExecutor;

        this.bloomIndex = new BloomIndex<>(bitmaps, Hashing.murmur3_128(), 100_000, 0.01f); // TODO fix somehow
    }

    public void index(final List<MiruActivityAndId<MiruActivity>> activityAndIds) throws Exception {
        @SuppressWarnings("unchecked")
        final List<MiruActivityAndId<MiruInternalActivity>> internalActivityAndIds = Arrays.<MiruActivityAndId<MiruInternalActivity>>asList(
            new MiruActivityAndId[activityAndIds.size()]);

        log.trace("Start: Index batch of {}", activityAndIds.size());

        final int numPartitions = 48;
        final int numActivities = internalActivityAndIds.size();
        final int partitionSize = (numActivities + numPartitions - 1) / numPartitions;

        List<Future<?>> internFutures = new ArrayList<>(numPartitions);
        for (int i = 0; i < activityAndIds.size(); i += partitionSize) {
            final int startOfSubList = i;
            internFutures.add(indexExecutor.submit(new Callable<Void>() {
                @Override
                public Void call() throws Exception {
                    activityInterner.intern(activityAndIds, startOfSubList, partitionSize, internalActivityAndIds, schema);
                    return null;
                }
            }));
        }
        awaitInternFutures(internFutures);

        List<Future<List<FieldValuesWork>>> fieldWorkFutures = composeFieldValuesWork(internalActivityAndIds);
        final List<Future<List<BloomWork>>> bloominsWorkFutures = composeBloominsWork(internalActivityAndIds);
        final List<Future<List<AggregateFieldsWork>>> aggregateFieldsWorkFutures = composeAggregateFieldsWork(internalActivityAndIds);

        List<FieldValuesWork>[] fieldsWork = awaitFieldWorkFutures(fieldWorkFutures);

        List<Future<?>> fieldFutures = indexFieldValues(fieldsWork);

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
                indexAuthz(internalActivityAndIds);
                return null;
            }
        }));
        otherFutures.addAll(indexAggregateFields(aggregateFieldsWork));
        otherFutures.addAll(indexBloomins(bloominsWork));
        otherFutures.addAll(indexWriteTimeAggregates(internalActivityAndIds));
        for (final List<MiruActivityAndId<MiruInternalActivity>> partition : Lists.partition(internalActivityAndIds, partitionSize)) {
            otherFutures.add(indexExecutor.submit(new Callable<Void>() {
                @Override
                public Void call() throws Exception {
                    activityIndex.set(partition);
                    return null;
                }
            }));
        }
        awaitOtherFutures(otherFutures);

        if (!activityAndIds.isEmpty()) {
            activityIndex.ready(activityAndIds.get(activityAndIds.size() - 1).id);
        }

        log.trace("End: Index batch of {}", activityAndIds.size());
    }

    public void set(List<MiruActivityAndId<MiruActivity>> activityAndIds) throws Exception {
        @SuppressWarnings("unchecked")
        List<MiruActivityAndId<MiruInternalActivity>> internalActivityAndIds = Arrays.<MiruActivityAndId<MiruInternalActivity>>asList(
            new MiruActivityAndId[activityAndIds.size()]);

        activityInterner.intern(activityAndIds, 0, activityAndIds.size(), internalActivityAndIds, schema);
        activityIndex.setAndReady(internalActivityAndIds);
    }

    public void repair(MiruActivity activity, int id) throws Exception {
        synchronized (stripingLocksProvider.lock(id)) {
            MiruInternalActivity existing = activityIndex.get(activity.tenantId, id);
            if (existing == null) {
                log.debug("Can't repair nonexistent activity at {}\n- offered: {}", id, activity);
            } else if (activity.version <= existing.version) {
                log.debug("Declined to repair old activity at {}\n- have: {}\n- offered: {}", id, existing, activity);
            } else {
                log.debug("Repairing activity at {}\n- was: {}\n- now: {}", id, existing, activity);
                @SuppressWarnings("unchecked")
                List<MiruActivityAndId<MiruInternalActivity>> internalActivityAndIds = Arrays.<MiruActivityAndId<MiruInternalActivity>>asList(
                    new MiruActivityAndId[1]);
                activityInterner.intern(Arrays.asList(new MiruActivityAndId<>(activity, id)), 0, 1, internalActivityAndIds, schema);

                for (MiruActivityAndId<MiruInternalActivity> internalActivityAndId : internalActivityAndIds) {
                    MiruInternalActivity internalActivity = internalActivityAndId.activity;
                    Set<String> existingAuthz = existing.authz != null ? Sets.newHashSet(existing.authz) : Sets.<String>newHashSet();
                    Set<String> repairedAuthz = internalActivity.authz != null ? Sets.newHashSet(internalActivity.authz) : Sets.<String>newHashSet();

                    for (String authz : existingAuthz) {
                        if (!repairedAuthz.contains(authz)) {
                            repairAuthz(authz, id, false);
                        }
                    }

                    for (String authz : repairedAuthz) {
                        if (!existingAuthz.contains(authz)) {
                            repairAuthz(authz, id, true);
                        }
                    }
                }

                //TODO repair fields?
                // repairs also unhide (remove from removal)
                removalIndex.remove(id);

                // finally, update the activity index
                activityIndex.setAndReady(internalActivityAndIds);
            }
        }
    }

    public void remove(MiruActivity activity, int id) throws Exception {
        synchronized (stripingLocksProvider.lock(id)) {
            MiruInternalActivity existing = activityIndex.get(activity.tenantId, id);
            if (existing == null) {
                log.debug("Can't remove nonexistent activity at {}\n- offered: {}", id, activity);
            } else if (activity.version <= existing.version) {
                log.debug("Declined to remove old activity at {}\n- have: {}\n- offered: {}", id, existing, activity);
            } else {
                log.debug("Removing activity at {}\n- was: {}\n- now: {}", id, existing, activity);
                @SuppressWarnings("unchecked")
                List<MiruActivityAndId<MiruInternalActivity>> internalActivity = Arrays.<MiruActivityAndId<MiruInternalActivity>>asList(
                    new MiruActivityAndId[1]);
                activityInterner.intern(Arrays.asList(new MiruActivityAndId<>(activity, id)), 0, 1, internalActivity, schema);

                //TODO apply field changes?
                // hide (add to removal)
                removalIndex.set(id);

                // finally, update the activity index
                activityIndex.setAndReady(internalActivity);
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

    private List<Future<List<FieldValuesWork>>> composeFieldValuesWork(final List<MiruActivityAndId<MiruInternalActivity>> internalActivityAndIds)
        throws Exception {

        List<Integer> fieldIds = schema.getFieldIds();
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

    private List<Future<?>> indexFieldValues(final List<FieldValuesWork>[] work) throws Exception {
        List<Integer> fieldIds = schema.getFieldIds();
        List<Future<?>> futures = new ArrayList<>(fieldIds.size());
        for (int fieldId = 0; fieldId < work.length; fieldId++) {
            final MiruField<BM> miruField = fieldIndex.getField(fieldId);
            List<FieldValuesWork> fieldWork = work[fieldId];
            for (final FieldValuesWork fieldValuesWork : fieldWork) {
                futures.add(indexExecutor.submit(new Callable<Void>() {
                    @Override
                    public Void call() throws Exception {
                        miruField.index(fieldValuesWork.fieldValue, fieldValuesWork.ids.toArray());
                        return null;
                    }
                }));
            }
        }
        return futures;
    }

    private void indexAuthz(List<MiruActivityAndId<MiruInternalActivity>> internalActivityAndIds) throws Exception {
        // TODO rewrite to use same pattern as indexFieldValues
        for (MiruActivityAndId<MiruInternalActivity> internalActivityAndId : internalActivityAndIds) {
            MiruInternalActivity activity = internalActivityAndId.activity;
            if (activity.authz != null) {
                for (String authz : activity.authz) {
                    authzIndex.index(authz, internalActivityAndId.id);
                }
            }
        }
    }

    private void repairAuthz(String authz, int id, boolean value) throws Exception {
        authzIndex.repair(authz, id, value);
    }

    private List<Future<List<BloomWork>>> composeBloominsWork(final List<MiruActivityAndId<MiruInternalActivity>> internalActivityAndIds) throws Exception {
        List<MiruFieldDefinition> fieldsWithBlooms = schema.getFieldsWithBlooms();

        List<Future<List<BloomWork>>> workFutures = Lists.newArrayList();
        for (final MiruFieldDefinition fieldDefinition : fieldsWithBlooms) {
            List<MiruFieldDefinition> bloominFieldDefinitions = schema.getBloominFieldDefinitions(fieldDefinition.fieldId);
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

    private List<Future<?>> indexBloomins(List<BloomWork> bloomWorks) {
        int callableCount = 0;
        List<Future<?>> futures = Lists.newArrayList();
        for (final BloomWork bloomWork : bloomWorks) {
            futures.add(indexExecutor.submit(new Callable<Void>() {
                @Override
                public Void call() throws Exception {
                    MiruField<BM> miruField = fieldIndex.getField(bloomWork.fieldId);
                    MiruFieldDefinition bloomFieldDefinition = schema.getFieldDefinition(bloomWork.bloomFieldId);
                    MiruTermId compositeBloomId = indexUtil.makeBloomComposite(bloomWork.fieldValue, bloomFieldDefinition.name);
                    MiruInvertedIndex<BM> invertedIndex = miruField.getOrCreateInvertedIndex(compositeBloomId);
                    bloomIndex.put(invertedIndex, bloomWork.bloomFieldValues);
                    return null;
                }
            }));
            callableCount++;
        }
        log.trace("Submitted {} bloom callables", callableCount);

        return futures;
    }

    private List<Future<?>> indexWriteTimeAggregates(List<MiruActivityAndId<MiruInternalActivity>> internalActivityAndIds) throws Exception {
        List<MiruFieldDefinition> writeTimeAggregateFields = schema.getWriteTimeAggregateFields();
        // rough estimate of necessary capacity
        List<Future<?>> futures = Lists.newArrayListWithCapacity(internalActivityAndIds.size() * writeTimeAggregateFields.size());
        for (final MiruActivityAndId<MiruInternalActivity> internalActivityAndId : internalActivityAndIds) {
            for (final MiruFieldDefinition fieldDefinition : writeTimeAggregateFields) {
                final MiruTermId[] fieldValues = internalActivityAndId.activity.fieldsValues[fieldDefinition.fieldId];
                if (fieldValues != null && fieldValues.length > 0) {
                    futures.add(indexExecutor.submit(new Callable<Void>() {
                        @Override
                        public Void call() throws Exception {
                            MiruField<BM> miruField = fieldIndex.getField(fieldDefinition.fieldId);

                            // Answers the question,
                            // "What is the latest activity against each distinct value of this field?"
                            MiruInvertedIndex<BM> aggregateIndex = miruField.getOrCreateInvertedIndex(fieldAggregateTermId);

                            // ["doc"] -> "d1", "d2", "d3", "d4" -> [0, 1(d1), 0, 0, 1(d2), 0, 0, 1(d3), 0, 0, 1(d4)]
                            for (MiruTermId fieldValue : fieldValues) {
                                Optional<MiruInvertedIndex<BM>> optionalFieldValueIndex = miruField.getInvertedIndex(fieldValue);
                                if (optionalFieldValueIndex.isPresent()) {
                                    MiruInvertedIndex<BM> fieldValueIndex = optionalFieldValueIndex.get();
                                    aggregateIndex.andNotToSourceSize(Collections.singletonList(fieldValueIndex.getIndexUnsafe()));
                                }
                            }
                            miruField.index(fieldAggregateTermId, internalActivityAndId.id);
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
    private List<Future<List<AggregateFieldsWork>>> composeAggregateFieldsWork(final List<MiruActivityAndId<MiruInternalActivity>> internalActivityAndIds)
        throws Exception {

        List<MiruFieldDefinition> fieldsWithAggregates = schema.getFieldsWithAggregates();

        List<Future<List<AggregateFieldsWork>>> workFutures = Lists.newArrayList();
        for (final MiruFieldDefinition fieldDefinition : fieldsWithAggregates) {
            List<MiruFieldDefinition> aggregateFieldDefinitions = schema.getAggregateFieldDefinitions(fieldDefinition.fieldId);
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

    private List<Future<?>> indexAggregateFields(List<AggregateFieldsWork> aggregateFieldsWorks) throws Exception {
        int callableCount = 0;
        List<Future<?>> futures = Lists.newArrayListWithCapacity(aggregateFieldsWorks.size());
        for (final AggregateFieldsWork aggregateFieldsWork : aggregateFieldsWorks) {
            futures.add(indexExecutor.submit(new Callable<Void>() {
                @Override
                public Void call() throws Exception {
                    MiruTermId fieldValue = aggregateFieldsWork.fieldValue;
                    List<IdAndTerm> idAndTerms = aggregateFieldsWork.work;

                    MiruField<BM> miruField = fieldIndex.getField(aggregateFieldsWork.fieldId);
                    MiruField<BM> aggregateField = fieldIndex.getField(aggregateFieldsWork.aggregateFieldId);
                    MiruFieldDefinition aggregateFieldDefinition = schema.getFieldDefinition(aggregateFieldsWork.aggregateFieldId);
                    MiruTermId compositeAggregateId = indexUtil.makeFieldValueAggregate(fieldValue, aggregateFieldDefinition.name);
                    MiruInvertedIndex<BM> invertedIndex = miruField.getOrCreateInvertedIndex(compositeAggregateId);

                    List<BM> aggregateBitmaps = Lists.newArrayListWithCapacity(idAndTerms.size());
                    TIntList ids = new TIntArrayList(idAndTerms.size());
                    for (IdAndTerm idAndTerm : idAndTerms) {
                        MiruTermId aggregateFieldValue = idAndTerm.term;
                        MiruInvertedIndex<BM> aggregateInvertedIndex = aggregateField.getOrCreateInvertedIndex(aggregateFieldValue);
                        BM aggregateBitmap = aggregateInvertedIndex.getIndexUnsafe();
                        aggregateBitmaps.add(aggregateBitmap);
                        ids.add(idAndTerm.id);
                    }

                    invertedIndex.andNotToSourceSize(aggregateBitmaps);

                    ids.reverse(); // we built in reverse order, so flip back to ascending
                    miruField.index(compositeAggregateId, ids.toArray());

                    return null;
                }
            }));
            callableCount++;
        }

        log.trace("Submitted {} aggregate field callables", callableCount);

        return futures;
    }

}
