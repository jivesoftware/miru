package com.jivesoftware.os.miru.service.stream;

import com.google.common.base.Optional;
import com.google.common.collect.Lists;
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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
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
    }

    public void index(final List<MiruActivityAndId<MiruActivity>> activityAndIds) throws Exception {
        final List<MiruActivityAndId<MiruInternalActivity>> internalActivityAndIds = Arrays.<MiruActivityAndId<MiruInternalActivity>>asList(
            new MiruActivityAndId[activityAndIds.size()]);

        log.trace("Start: Index batch of {}", activityAndIds.size());

        final int numPartitions = 24;
        final int numActivities = internalActivityAndIds.size();
        final int partitionSize = (numActivities + numPartitions - 1) / numPartitions;

        List<Future<?>> internFutures = new ArrayList<>(numPartitions);
        for (int i = 0; i < activityAndIds.size(); i += partitionSize) {
            final int startOfSubList = i;
            internFutures.add(indexExecutor.submit(new Callable<Void>() {

                @Override
                public Void call() throws Exception {
                    log.trace("Start: Intern {}", startOfSubList);
                    activityInterner.intern(activityAndIds, startOfSubList, partitionSize, internalActivityAndIds, schema);
                    log.trace("End: Intern {}", startOfSubList);
                    return null;
                }
            }));
        }
        for (Future future : internFutures) {
            future.get();
        }
        log.trace("Finished waiting for interns");

        List<Future<?>> fieldFutures = indexFieldValues(internalActivityAndIds);
        for (Future future : fieldFutures) {
            future.get();
        }
        log.trace("Finished waiting for fields");

        final List<Future<?>> indexFutures = new ArrayList<>();
        indexFutures.add(indexExecutor.submit(new Callable<Void>() {

            @Override
            public Void call() throws Exception {
                log.trace("Start: Authz");
                indexAuthz(internalActivityAndIds);
                log.trace("End: Authz");
                return null;
            }
        }));

        indexFutures.addAll(indexBloomins(internalActivityAndIds));
        indexFutures.addAll(indexWriteTimeAggregates(internalActivityAndIds));

        for (Future future : indexFutures) {
            future.get();
        }
        log.trace("Finished waiting for authz/blooms/aggregates");

        List<Future<?>> activityFutures = new ArrayList<>(numPartitions);
        for (final List<MiruActivityAndId<MiruInternalActivity>> partition : Lists.partition(internalActivityAndIds, partitionSize)) {
            activityFutures.add(indexExecutor.submit(new Callable<Void>() {
                @Override
                public Void call() throws Exception {
                    activityIndex.set(partition);
                    return null;
                }
            }));
        }
        for (Future future : activityFutures) {
            future.get();
        }
        log.trace("Finished waiting for activity index");

        if (!activityAndIds.isEmpty()) {
            activityIndex.ready(activityAndIds.get(activityAndIds.size() - 1).id);
        }
    }

    public void set(List<MiruActivityAndId<MiruActivity>> activityAndIds) throws Exception {
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

    private List<Future<?>> indexFieldValues(final List<MiruActivityAndId<MiruInternalActivity>> internalActivityAndIds) throws Exception {
        List<Integer> fieldIds = schema.getFieldIds();
        List<Future<?>> futures = new ArrayList<>(fieldIds.size());
        for (final int fieldId : fieldIds) {
            futures.add(indexExecutor.submit(new Callable<Void>() {
                @Override
                public Void call() throws Exception {
                    log.trace("Start: Index field {}", fieldId);
                    MiruField<BM> miruField = null;
                    for (MiruActivityAndId<MiruInternalActivity> internalActivityAndId : internalActivityAndIds) {
                        MiruInternalActivity activity = internalActivityAndId.activity;
                        if (activity.fieldsValues[fieldId] != null) {
                            if (miruField == null) {
                                miruField = fieldIndex.getField(fieldId);
                            }
                            for (MiruTermId term : activity.fieldsValues[fieldId]) {
                                miruField.index(term, internalActivityAndId.id);
                            }
                        }
                    }
                    log.trace("End: Index field {}", fieldId);
                    return null;
                }
            }));
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

    private List<Future<?>> indexBloomins(List<MiruActivityAndId<MiruInternalActivity>> internalActivityAndIds) throws Exception {
        List<Future<?>> futures = Lists.newArrayList();
        BloomIndex<BM> bloomIndex = null;
        int bloominCallableCount = 0;
        List<Integer> fieldIds = schema.getFieldIds();
        for (MiruActivityAndId<MiruInternalActivity> internalActivityAndId : internalActivityAndIds) {
            MiruInternalActivity activity = internalActivityAndId.activity;
            for (final int fieldId : fieldIds) {
                List<MiruFieldDefinition> bloominFieldDefinitions = schema.getBloominFieldDefinitions(fieldId);
                for (final MiruFieldDefinition bloominFieldDefinition : bloominFieldDefinitions) {
                    MiruTermId[] fieldValues = activity.fieldsValues[fieldId];
                    if (fieldValues != null && fieldValues.length > 0) {
                        final MiruTermId[] bloomFieldValues = activity.fieldsValues[bloominFieldDefinition.fieldId];
                        if (bloomFieldValues != null && bloomFieldValues.length > 0) {
                            for (final MiruTermId fieldValue : fieldValues) {
                                if (bloomIndex == null) {
                                    bloomIndex = new BloomIndex<>(bitmaps, Hashing.murmur3_128(), 100_000, 0.01f); // TODO fix somehow
                                }

                                final BloomIndex<BM> callableBloomIndex = bloomIndex;
                                futures.add(indexExecutor.submit(new Callable<Void>() {
                                    @Override
                                    public Void call() throws Exception {
                                        MiruField<BM> miruField = fieldIndex.getField(fieldId);
                                        MiruTermId compositeBloomId = indexUtil.makeBloomComposite(fieldValue, bloominFieldDefinition.name);
                                        MiruInvertedIndex<BM> invertedIndex = miruField.getOrCreateInvertedIndex(compositeBloomId);
                                        callableBloomIndex.put(invertedIndex, bloomFieldValues);
                                        return null;
                                    }
                                }));

                                bloominCallableCount++;
                            }
                        }
                    }
                }
            }
        }
        log.trace("Dispatched {} bloomin callables", bloominCallableCount);

        return futures;
    }

    private List<Future<?>> indexWriteTimeAggregates(final List<MiruActivityAndId<MiruInternalActivity>> internalActivityAndIds) throws Exception {

        List<Integer> fieldIds = schema.getFieldIds();
        List<Future<?>> futures = new ArrayList<>(fieldIds.size() * 2);
        for (final MiruActivityAndId<MiruInternalActivity> internalActivityAndId : internalActivityAndIds) {
            for (final int fieldId : fieldIds) {
                MiruFieldDefinition fieldDefinition = schema.getFieldDefinition(fieldId);
                if (fieldDefinition.writeTimeAggregate) {
                    final MiruTermId[] fieldValues = internalActivityAndId.activity.fieldsValues[fieldId];
                    if (fieldValues != null && fieldValues.length > 0) {
                        futures.add(indexExecutor.submit(new Callable<Void>() {
                            @Override
                            public Void call() throws Exception {
                                MiruField<BM> miruField = fieldIndex.getField(fieldId);

                                // Answers the question,
                                // "What is the latest activity against each distinct value of this field?"
                                MiruTermId miruTermId = indexUtil.makeFieldAggregate();
                                MiruInvertedIndex<BM> aggregateIndex = miruField.getOrCreateInvertedIndex(miruTermId);

                                // ["doc"] -> "d1", "d2", "d3", "d4" -> [0, 1(d1), 0, 0, 1(d2), 0, 0, 1(d3), 0, 0, 1(d4)]
                                for (MiruTermId fieldValue : fieldValues) {
                                    Optional<MiruInvertedIndex<BM>> optionalFieldValueIndex = miruField.getInvertedIndex(fieldValue);
                                    if (optionalFieldValueIndex.isPresent()) {
                                        MiruInvertedIndex<BM> fieldValueIndex = optionalFieldValueIndex.get();
                                        aggregateIndex.andNotToSourceSize(fieldValueIndex.getIndex());
                                    }
                                }
                                miruField.index(miruTermId, internalActivityAndId.id);
                                return null;
                            }
                        }));
                    }
                }
            }
        }

        int aggregateFieldCallableCount = 0;
        Set<WriteAggregateKey> visited = Sets.newHashSet();
        // walk backwards so we see the largest id first, and mark visitors for each coordinate
        for (int i = internalActivityAndIds.size() - 1; i >= 0; i--) {
            final MiruActivityAndId<MiruInternalActivity> internalActivityAndId = internalActivityAndIds.get(i);
            MiruInternalActivity activity = internalActivityAndId.activity;
            for (final int fieldId : fieldIds) {
                MiruTermId[] fieldValues = activity.fieldsValues[fieldId];
                if (fieldValues != null && fieldValues.length > 0) {
                    List<MiruFieldDefinition> aggregateFieldDefinitions = schema.getAggregateFieldDefinitions(fieldId);
                    for (final MiruFieldDefinition aggregateFieldDefinition : aggregateFieldDefinitions) {
                        // Answers the question,
                        // "For each distinct value of this field, what is the latest activity against each distinct value of the related field?"
                        MiruTermId[] aggregateFieldValues = activity.fieldsValues[aggregateFieldDefinition.fieldId];
                        if (aggregateFieldValues != null && aggregateFieldValues.length > 0) {
                            for (MiruTermId aggregateFieldValue : aggregateFieldValues) {
                                MiruInvertedIndex<BM> aggregateInvertedIndex = null;
                                for (final MiruTermId fieldValue : fieldValues) {
                                    WriteAggregateKey key = new WriteAggregateKey(aggregateFieldDefinition.fieldId, aggregateFieldValue, fieldValue);
                                    if (visited.add(key)) {
                                        MiruField<BM> aggregateField = fieldIndex.getField(aggregateFieldDefinition.fieldId);
                                        if (aggregateInvertedIndex == null) {
                                            aggregateInvertedIndex = aggregateField.getOrCreateInvertedIndex(aggregateFieldValue);
                                        }

                                        final MiruInvertedIndex<BM> callableAggregateInvertedIndex = aggregateInvertedIndex;
                                        futures.add(indexExecutor.submit(new Callable<Void>() {
                                            @Override
                                            public Void call() throws Exception {
                                                MiruField<BM> miruField = fieldIndex.getField(fieldId);
                                                MiruTermId compositeAggregateId = indexUtil.makeFieldValueAggregate(fieldValue, aggregateFieldDefinition.name);
                                                MiruInvertedIndex<BM> invertedIndex = miruField.getOrCreateInvertedIndex(compositeAggregateId);
                                                invertedIndex.andNotToSourceSize(callableAggregateInvertedIndex.getIndex());
                                                miruField.index(compositeAggregateId, internalActivityAndId.id);
                                                return null;
                                            }
                                        }));

                                        aggregateFieldCallableCount++;
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
        log.trace("Dispatched {} aggregate field callables", aggregateFieldCallableCount);
        return futures;
    }

    private static class WriteAggregateKey {
        public final int aggregateFieldId;
        public final MiruTermId aggregateFieldValue;
        public final MiruTermId fieldValue;

        private WriteAggregateKey(int aggregateFieldId, MiruTermId aggregateFieldValue, MiruTermId fieldValue) {
            this.aggregateFieldId = aggregateFieldId;
            this.aggregateFieldValue = aggregateFieldValue;
            this.fieldValue = fieldValue;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            WriteAggregateKey that = (WriteAggregateKey) o;

            if (aggregateFieldId != that.aggregateFieldId) {
                return false;
            }
            if (aggregateFieldValue != null ? !aggregateFieldValue.equals(that.aggregateFieldValue) : that.aggregateFieldValue != null) {
                return false;
            }
            if (fieldValue != null ? !fieldValue.equals(that.fieldValue) : that.fieldValue != null) {
                return false;
            }

            return true;
        }

        @Override
        public int hashCode() {
            int result = aggregateFieldId;
            result = 31 * result + (aggregateFieldValue != null ? aggregateFieldValue.hashCode() : 0);
            result = 31 * result + (fieldValue != null ? fieldValue.hashCode() : 0);
            return result;
        }
    }
}
