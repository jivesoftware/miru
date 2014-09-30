package com.jivesoftware.os.miru.service.stream;

import com.google.common.base.Optional;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Sets;
import com.google.common.collect.Table;
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
import java.util.Map.Entry;
import java.util.Set;

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
    private final MiruIndexUtil indexUtil = new MiruIndexUtil();
    private final MiruTermId fieldAggregateTermId = indexUtil.makeFieldAggregate();

    private final StripingLocksProvider<Integer> stripingLocksProvider = new StripingLocksProvider<>(64);// TODO expose to config

    public MiruIndexContext(MiruBitmaps<BM> bitmaps,
        MiruSchema schema,
        MiruActivityIndex activityIndex,
        MiruFields<BM> fieldIndex,
        MiruAuthzIndex authzIndex,
        MiruRemovalIndex removalIndex,
        MiruActivityInternExtern activityInterner) {
        this.bitmaps = bitmaps;
        this.schema = schema;
        this.activityIndex = activityIndex;
        this.fieldIndex = fieldIndex;
        this.authzIndex = authzIndex;
        this.removalIndex = removalIndex;
        this.activityInterner = activityInterner;
    }

    public void index(List<MiruActivityAndId<MiruActivity>> activityAndIds) throws Exception {
        List<MiruActivityAndId<MiruInternalActivity>> internalActivityAndIds = activityInterner.intern(activityAndIds, schema);
        try {
            log.startNanoTimer("indexing>indexFieldValues");
            indexFieldValues(internalActivityAndIds);
        } finally {
            log.stopNanoTimer("indexing>indexFieldValues");
        }
        try {
            log.startNanoTimer("indexing>indexAuthz");
            indexAuthz(internalActivityAndIds);
        } finally {
            log.stopNanoTimer("indexing>indexAuthz");
        }
        try {
            log.startNanoTimer("indexing>indexBloomins");
            indexBloomins(internalActivityAndIds);
        } finally {
            log.stopNanoTimer("indexing>indexBloomins");
        }
        try {
            log.startNanoTimer("indexing>indexWriteTimeAggregates");
            indexWriteTimeAggregates(internalActivityAndIds);
        } finally {
            log.stopNanoTimer("indexing>indexWriteTimeAggregates");
        }
        // add to the activity index last, and use it as the ultimate indicator of whether an activity is fully indexed
        try {
            log.startNanoTimer("indexing>activityIndex");
            activityIndex.set(internalActivityAndIds);
        } finally {
            log.stopNanoTimer("indexing>activityIndex");
        }
    }

    public void set(List<MiruActivityAndId<MiruActivity>> activityAndIds) {
        List<MiruActivityAndId<MiruInternalActivity>> internalActivityAndIds = activityInterner.intern(activityAndIds, schema);
        activityIndex.set(internalActivityAndIds);
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
                List<MiruActivityAndId<MiruInternalActivity>> internalActivityAndIds = activityInterner
                    .intern(Arrays.asList(new MiruActivityAndId<>(activity, id)), schema);

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
                activityIndex.set(internalActivityAndIds);
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
                List<MiruActivityAndId<MiruInternalActivity>> internalActivity = activityInterner
                    .intern(Arrays.asList(new MiruActivityAndId<>(activity, id)), schema);

                //TODO apply field changes?
                // hide (add to removal)
                removalIndex.set(id);

                // finally, update the activity index
                activityIndex.set(internalActivity);
            }
        }
    }

    private void indexFieldValues(List<MiruActivityAndId<MiruInternalActivity>> internalActivityAndIds) throws Exception {
        Table<Integer, MiruTermId, List<Integer>> state = HashBasedTable.create();
        for (MiruActivityAndId<MiruInternalActivity> internalActivityAndId : internalActivityAndIds) {
            MiruInternalActivity activity = internalActivityAndId.activity;
            for (int fieldId = 0; fieldId < activity.fieldsValues.length; fieldId++) {
                if (activity.fieldsValues[fieldId] != null) {
                    for (MiruTermId term : activity.fieldsValues[fieldId]) {
                        List<Integer> ids = state.get(fieldId, term);
                        if (ids == null) {
                            ids = new ArrayList<>(internalActivityAndIds.size());
                            state.put(fieldId, term, ids);
                        }
                        ids.add(internalActivityAndId.id);
                    }
                }
            }
        }

        for (Integer fieldId : state.rowKeySet()) {
            MiruField<BM> miruField = fieldIndex.getField(fieldId);
            for (Entry<MiruTermId, List<Integer>> entry : state.row(fieldId).entrySet()) {
                MiruTermId fieldValue = entry.getKey();
                for (Integer id : entry.getValue()) {
                    miruField.index(fieldValue, id);
                }
            }
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

    private void indexWriteTimeAggregates(List<MiruActivityAndId<MiruInternalActivity>> internalActivityAndIds) throws Exception {

        Table<Integer, MiruTermId, Integer> state = HashBasedTable.create();
        Table<Integer, MiruTermId, Table<Integer, MiruTermId, Integer>> aggregatState = HashBasedTable.create();
        for (MiruActivityAndId<MiruInternalActivity> internalActivityAndId : internalActivityAndIds) {
            MiruInternalActivity activity = internalActivityAndId.activity;
            for (int fieldId = 0; fieldId < activity.fieldsValues.length; fieldId++) {
                MiruField<BM> miruField = fieldIndex.getField(fieldId);
                MiruTermId[] fieldValues = activity.fieldsValues[fieldId];

                if (fieldValues != null && fieldValues.length > 0) {

                    // Builds up the state to answer the question,
                    // "What is the latest activity against each distinct value of this field?"
                    if (miruField.getFieldDefinition().writeTimeAggregate) {
                        for (MiruTermId fieldValue : fieldValues) {
                            state.put(fieldId, fieldValue, internalActivityAndId.id);
                        }
                    }

                    // Builds the question,
                    // "For each distinct value of this field, what is the latest activity against each distinct value of the related field?"
                    for (String aggregateFieldName : miruField.getFieldDefinition().aggregateFieldNames) {
                        int aggregateFieldId = schema.getFieldId(aggregateFieldName);
                        if (aggregateFieldId >= 0) {
                            MiruTermId[] aggregateFieldValues = activity.fieldsValues[aggregateFieldId];
                            if (aggregateFieldValues != null && aggregateFieldValues.length > 0) {
                                for (MiruTermId aggregateFieldValue : aggregateFieldValues) {
                                    for (MiruTermId fieldValue : fieldValues) {
                                        Table<Integer, MiruTermId, Integer> got = aggregatState.get(aggregateFieldId, aggregateFieldValue);
                                        if (got == null) {
                                            got = HashBasedTable.create();
                                            aggregatState.put(aggregateFieldId, aggregateFieldValue, got);
                                        }
                                        got.put(fieldId, fieldValue, internalActivityAndId.id);
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }

        // Answers the question,
        // "What is the latest activity against each distinct value of this field?"
        for (Integer fieldId : state.rowKeySet()) {
            MiruField<BM> miruField = fieldIndex.getField(fieldId);
            MiruInvertedIndex<BM> aggregateIndex = miruField.getOrCreateInvertedIndex(fieldAggregateTermId);
            for (Entry<MiruTermId, Integer> entry : state.row(fieldId).entrySet()) {
                MiruTermId fieldValue = entry.getKey();
                Optional<MiruInvertedIndex<BM>> optionalFieldValueIndex = miruField.getInvertedIndex(fieldValue);
                if (optionalFieldValueIndex.isPresent()) {
                    MiruInvertedIndex<BM> fieldValueIndex = optionalFieldValueIndex.get();
                    aggregateIndex.andNotToSourceSize(fieldValueIndex.getIndex());
                }
                miruField.index(fieldAggregateTermId, entry.getValue());
            }
        }

        // Answers the question,
        // "For each distinct value of this field, what is the latest activity against each distinct value of the related field?"
        for (Integer aggregateFieldId : aggregatState.rowKeySet()) {
            MiruField<BM> aggregateField = fieldIndex.getField(aggregateFieldId);
            for (Entry<MiruTermId, Table<Integer, MiruTermId, Integer>> aggragateEntry : aggregatState.row(aggregateFieldId).entrySet()) {
                MiruTermId aggregateFieldValue = aggragateEntry.getKey();
                MiruInvertedIndex<BM> aggregateInvertedIndex = aggregateField.getOrCreateInvertedIndex(aggregateFieldValue);

                Table<Integer, MiruTermId, Integer> fieldState = aggragateEntry.getValue();
                for (Integer fieldId : fieldState.rowKeySet()) {
                    MiruField<BM> miruField = fieldIndex.getField(fieldId);
                    for (Entry<MiruTermId, Integer> fieldEntry : fieldState.row(fieldId).entrySet()) {
                        MiruTermId fieldValue = fieldEntry.getKey();

                        MiruTermId compositeAggregateId = indexUtil.makeFieldValueAggregate(fieldValue, aggregateField.getFieldDefinition().name);
                        MiruInvertedIndex<BM> invertedIndex = miruField.getOrCreateInvertedIndex(compositeAggregateId);

                        invertedIndex.andNotToSourceSize(aggregateInvertedIndex.getIndex());
                        miruField.index(compositeAggregateId, fieldEntry.getValue());
                    }
                }
            }
        }

    }
}
