package com.jivesoftware.os.miru.service.stream;

import com.google.common.base.Optional;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.hash.Hashing;
import com.jivesoftware.os.jive.utils.base.util.locks.StripingLocksProvider;
import com.jivesoftware.os.jive.utils.logger.MetricLogger;
import com.jivesoftware.os.jive.utils.logger.MetricLoggerFactory;
import com.jivesoftware.os.miru.api.activity.MiruActivity;
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

        final int batchSize = 1000; // TODO expose to config
        List<Future<?>> internFutures = new ArrayList<>(activityAndIds.size() / batchSize);
        for (int i = 0; i < activityAndIds.size(); i += batchSize) {
            final int startOfSubList = i;
            internFutures.add(indexExecutor.submit(new Callable<Void>() {

                @Override
                public Void call() throws Exception {
                    activityInterner.intern(activityAndIds, startOfSubList, batchSize, internalActivityAndIds, schema);
                    return null;
                }
            }));
        }
        for (Future future : internFutures) {
            future.get();
        }

        indexFieldValues(internalActivityAndIds);

        final List<Future<?>> indexFutures = new ArrayList<>();
        indexFutures.add(indexExecutor.submit(new Callable<Void>() {

            @Override
            public Void call() throws Exception {
                indexAuthz(internalActivityAndIds);
                return null;
            }
        }));
        indexFutures.add(indexExecutor.submit(new Callable<Void>() {

            @Override
            public Void call() throws Exception {
                indexBloomins(internalActivityAndIds);
                return null;
            }
        }));

        indexFutures.addAll(indexWriteTimeAggregates(internalActivityAndIds));

        for (Future future : indexFutures) {
            future.get();
        }

        int numActivities = internalActivityAndIds.size();
        int numPartitions = 10;
        int partitionSize = (numActivities + numPartitions - 1) / numPartitions;
        List<Future<?>> activityFutures = new ArrayList<>(partitionSize);
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

    private void indexFieldValues(final List<MiruActivityAndId<MiruInternalActivity>> internalActivityAndIds) throws Exception {
        List<Integer> fieldIds = schema.getFieldIds();
        List<Future> futures = new ArrayList<>(fieldIds.size());
        for (final int fieldId : fieldIds) {
            futures.add(indexExecutor.submit(new Callable<Void>() {
                @Override
                public Void call() throws Exception {
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
                    return null;
                }
            }));
        }

        for (Future future : futures) {
            future.get();
        }
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

    private void indexBloomins(List<MiruActivityAndId<MiruInternalActivity>> internalActivityAndIds) throws Exception {
        BloomIndex<BM> bloomIndex = new BloomIndex<>(bitmaps, Hashing.murmur3_128(), 100_000, 0.01f); // TODO fix so how
        for (MiruActivityAndId<MiruInternalActivity> internalActivityAndId : internalActivityAndIds) {
            MiruInternalActivity activity = internalActivityAndId.activity;
            for (int fieldId = 0; fieldId < activity.fieldsValues.length; fieldId++) {
                MiruField<BM> miruField = fieldIndex.getField(fieldId);
                List<String> bloomsFieldNames = miruField.getFieldDefinition().bloomFieldNames;
                MiruTermId[] fieldValues = activity.fieldsValues[fieldId];
                if (fieldValues != null && fieldValues.length > 0) {
                    for (String bloomsFieldName : bloomsFieldNames) {
                        int bloomsFieldId = schema.getFieldId(bloomsFieldName);
                        MiruTermId[] bloomFieldValues = activity.fieldsValues[bloomsFieldId];
                        if (bloomFieldValues != null && bloomFieldValues.length > 0) {
                            for (MiruTermId fieldValue : fieldValues) {
                                MiruTermId compositeBloomId = indexUtil.makeBloomComposite(fieldValue, bloomsFieldName);
                                MiruInvertedIndex<BM> invertedIndex = miruField.getOrCreateInvertedIndex(compositeBloomId);
                                bloomIndex.put(invertedIndex, bloomFieldValues);
                            }
                        }
                    }
                }
            }
        }
    }

    private List<Future<?>> indexWriteTimeAggregates(final List<MiruActivityAndId<MiruInternalActivity>> internalActivityAndIds) throws Exception {
        List<Integer> fieldIds = schema.getFieldIds();
        List<Future<?>> futures = new ArrayList<>(fieldIds.size());
        for (final int fieldId : fieldIds) {
            futures.add(indexExecutor.submit(new Callable<Void>() {
                @Override
                public Void call() throws Exception {
                    MiruField<BM> miruField = null;
                    for (MiruActivityAndId<MiruInternalActivity> internalActivityAndId : internalActivityAndIds) {
                        MiruInternalActivity activity = internalActivityAndId.activity;
                        MiruTermId[] fieldValues = activity.fieldsValues[fieldId];
                        if (fieldValues != null && fieldValues.length > 0) {
                            if (miruField == null) {
                                miruField = fieldIndex.getField(fieldId);
                            }

                            // Answers the question,
                            // "What is the latest activity against each distinct value of this field?"
                            boolean writeTimeAggregate = miruField.getFieldDefinition().writeTimeAggregate;
                            if (writeTimeAggregate) {
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
                            }

                            // Answers the question,
                            // "For each distinct value of this field, what is the latest activity against each distinct value of the related field?"
                            for (String aggregateFieldName : miruField.getFieldDefinition().aggregateFieldNames) {
                                int aggregateFieldId = schema.getFieldId(aggregateFieldName);
                                if (aggregateFieldId >= 0) {
                                    MiruField<BM> aggregateField = fieldIndex.getField(aggregateFieldId);
                                    MiruTermId[] aggregateFieldValues = activity.fieldsValues[aggregateFieldId];
                                    if (aggregateFieldValues != null && aggregateFieldValues.length > 0) {
                                        for (MiruTermId aggregateFieldValue : aggregateFieldValues) {
                                            MiruInvertedIndex<BM> aggregateInvertedIndex = aggregateField.getOrCreateInvertedIndex(aggregateFieldValue);
                                            for (MiruTermId fieldValue : fieldValues) {
                                                MiruTermId compositeAggregateId = indexUtil.makeFieldValueAggregate(fieldValue, aggregateFieldName);
                                                MiruInvertedIndex<BM> invertedIndex = miruField.getOrCreateInvertedIndex(compositeAggregateId);

                                                invertedIndex.andNotToSourceSize(aggregateInvertedIndex.getIndex());
                                                miruField.index(compositeAggregateId, internalActivityAndId.id);
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                    return null;
                }
            }));
        }
        return futures;
    }
}
