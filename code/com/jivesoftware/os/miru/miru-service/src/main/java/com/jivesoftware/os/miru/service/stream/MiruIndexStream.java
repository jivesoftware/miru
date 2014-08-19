package com.jivesoftware.os.miru.service.stream;

import com.google.common.base.Optional;
import com.google.common.collect.Sets;
import com.google.common.hash.Hashing;
import com.google.common.primitives.Bytes;
import com.jivesoftware.os.jive.utils.base.util.locks.StripingLocksProvider;
import com.jivesoftware.os.jive.utils.logger.MetricLogger;
import com.jivesoftware.os.jive.utils.logger.MetricLoggerFactory;
import com.jivesoftware.os.miru.api.activity.MiruActivity;
import com.jivesoftware.os.miru.api.base.MiruTermId;
import com.jivesoftware.os.miru.service.index.BloomIndex;
import com.jivesoftware.os.miru.service.index.MiruField;
import com.jivesoftware.os.miru.service.index.MiruFields;
import com.jivesoftware.os.miru.service.index.MiruInvertedIndex;
import com.jivesoftware.os.miru.service.index.MiruRemovalIndex;
import com.jivesoftware.os.miru.service.index.MiruActivityIndex;
import com.jivesoftware.os.miru.service.index.auth.MiruAuthzIndex;
import com.jivesoftware.os.miru.service.schema.MiruSchema;

import java.util.List;
import java.util.Set;

/**
 * Handles indexing of activity, including repair and removal, with synchronization and attention to versioning.
 */
public class MiruIndexStream {

    private final static MetricLogger log = MetricLoggerFactory.getLogger();

    private final MiruSchema schema;
    private final MiruActivityIndex activityIndex;
    private final MiruFields fieldIndex;
    private final MiruAuthzIndex authzIndex;
    private final MiruRemovalIndex removalIndex;
    private final MiruActivityInterner activityInterner;

    private final StripingLocksProvider<Integer> stripingLocksProvider = new StripingLocksProvider<>(64);

    public MiruIndexStream(MiruSchema schema,
            MiruActivityIndex activityIndex,
            MiruFields fieldIndex,
            MiruAuthzIndex authzIndex,
            MiruRemovalIndex removalIndex,
            MiruActivityInterner activityInterner) {
        this.schema = schema;
        this.activityIndex = activityIndex;
        this.fieldIndex = fieldIndex;
        this.authzIndex = authzIndex;
        this.removalIndex = removalIndex;
        this.activityInterner = activityInterner;
    }

    public void index(MiruActivity activity, int id) throws Exception {
        activity = activityInterner.intern(activity);
        indexFieldValues(activity, id);
        indexAuthz(activity, id);
        indexBloomins(activity);
        indexWriteTimeAggregates(activity, id);
        // add to the activity index last, and use it as the ultimate indicator of whether an activity is fully indexed
        set(activity, id);
    }

    public void set(MiruActivity activity, int id) {
        activity = activityInterner.intern(activity);
        activityIndex.set(id, activity);
    }

    public void repair(MiruActivity activity, int id) throws Exception {
        synchronized (stripingLocksProvider.lock(id)) {
            MiruActivity existing = activityIndex.get(id);
            if (existing == null) {
                log.debug("Can't repair nonexistent activity at {}\n- offered: {}", id, activity);
            } else if (activity.version <= existing.version) {
                log.debug("Declined to repair old activity at {}\n- have: {}\n- offered: {}", id, existing, activity);
            } else {
                log.debug("Repairing activity at {}\n- was: {}\n- now: {}", id, existing, activity);
                activity = activityInterner.intern(activity);

                Set<String> existingAuthz = existing.authz != null ? Sets.newHashSet(existing.authz) : Sets.<String>newHashSet();
                Set<String> repairedAuthz = activity.authz != null ? Sets.newHashSet(activity.authz) : Sets.<String>newHashSet();

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

                //TODO repair fields?
                // repairs also unhide (remove from removal)
                removalIndex.remove(id);

                // finally, update the activity index
                activityIndex.set(id, activity);
            }
        }
    }

    public void remove(MiruActivity activity, int id) throws Exception {
        synchronized (stripingLocksProvider.lock(id)) {
            MiruActivity existing = activityIndex.get(id);
            if (existing == null) {
                log.debug("Can't remove nonexistent activity at {}\n- offered: {}", id, activity);
            } else if (activity.version <= existing.version) {
                log.debug("Declined to remove old activity at {}\n- have: {}\n- offered: {}", id, existing, activity);
            } else {
                log.debug("Removing activity at {}\n- was: {}\n- now: {}", id, existing, activity);
                activity = activityInterner.intern(activity);

                //TODO apply field changes?
                // hide (add to removal)
                removalIndex.set(id);

                // finally, update the activity index
                activityIndex.set(id, activity);
            }
        }
    }

    private void indexFieldValues(final MiruActivity activity, final int id) throws Exception {
        for (String fieldName : activity.fieldsValues.keySet()) {
            int fieldId = schema.getFieldId(fieldName);
            if (fieldId >= 0) {
                MiruField miruField = fieldIndex.getField(fieldId);
                for (MiruTermId term : activity.fieldsValues.get(fieldName)) {
                    miruField.index(term, id);
                }
            }
        }
    }

    private void indexAuthz(MiruActivity activity, int id) throws Exception {
        if (activity.authz != null) {
            for (String authz : activity.authz) {
                authzIndex.index(authz, id);
            }
        }
    }

    private void repairAuthz(String authz, int id, boolean value) throws Exception {
        authzIndex.repair(authz, id, value);
    }

    private void indexBloomins(MiruActivity activity) throws Exception {
        BloomIndex bloomIndex = new BloomIndex(Hashing.murmur3_128(), 100000, 0.01f); // TODO fix so how


        for (String fieldName : activity.fieldsValues.keySet()) {
            int fieldId = schema.getFieldId(fieldName);
            if (fieldId >= 0) {
                MiruField miruField = fieldIndex.getField(fieldId);
                List<String> bloomsFieldNames = miruField.getFieldDefinition().bloomFieldNames;
                MiruTermId[] fieldValues = activity.fieldsValues.get(fieldName);
                if (fieldValues != null && fieldValues.length > 0) {
                    for (String bloomsFieldName : bloomsFieldNames) {
                        MiruTermId[] bloomFieldValues = activity.fieldsValues.get(bloomsFieldName);
                        if (bloomFieldValues != null && bloomFieldValues.length > 0) {
                            for (MiruTermId fieldValue : fieldValues) {
                                MiruTermId compositeBloomId = makeComposite(fieldValue, "|", bloomsFieldName);
                                MiruInvertedIndex invertedIndex = miruField.getOrCreateInvertedIndex(compositeBloomId);
                                bloomIndex.put(invertedIndex, bloomFieldValues);
                            }
                        }
                    }
                }
            }
        }

    }

    private void indexWriteTimeAggregates(MiruActivity activity, int id) throws Exception {
        for (String fieldName : activity.fieldsValues.keySet()) {
            int fieldId = schema.getFieldId(fieldName);
            if (fieldId >= 0) {
                MiruField miruField = fieldIndex.getField(fieldId);
                MiruTermId[] fieldValues = activity.fieldsValues.get(fieldName);
                if (fieldValues != null && fieldValues.length > 0) {
                    // Answers the question,
                    // "What is the latest activity against each distinct value of this field?"
                    boolean writeTimeAggregate = miruField.getFieldDefinition().writeTimeAggregate;
                    if (writeTimeAggregate) {
                        MiruTermId miruTermId = makeComposite(new MiruTermId(MiruSchema.RESERVED_AGGREGATE.getBytes()), "~", MiruSchema.RESERVED_AGGREGATE);
                        MiruInvertedIndex aggregateIndex = miruField.getOrCreateInvertedIndex(miruTermId);

                        // ["doc"] -> "d1", "d2", "d3", "d4" -> [0, 1(d1), 0, 0, 1(d2), 0, 0, 1(d3), 0, 0, 1(d4)]
                        for (MiruTermId fieldValue : fieldValues) {
                            Optional<MiruInvertedIndex> optionalFieldValueIndex = miruField.getInvertedIndex(fieldValue);
                            if (optionalFieldValueIndex.isPresent()) {
                                MiruInvertedIndex fieldValueIndex = optionalFieldValueIndex.get();
                                aggregateIndex.andNotToSourceSize(fieldValueIndex.getIndex());
                            }
                        }
                        miruField.index(miruTermId, id);
                    }

                    // Answers the question,
                    // "For each distinct value of this field, what is the latest activity against each distinct value of the related field?"
                    for (String aggregateFieldName : miruField.getFieldDefinition().aggregateFieldNames) {
                        int aggregateFieldId = schema.getFieldId(aggregateFieldName);
                        if (aggregateFieldId >= 0) {
                            MiruField aggregateField = fieldIndex.getField(aggregateFieldId);
                            MiruTermId[] aggregateFieldValues = activity.fieldsValues.get(aggregateFieldName);
                            if (aggregateFieldValues != null && aggregateFieldValues.length > 0) {
                                for (MiruTermId aggregateFieldValue : aggregateFieldValues) {
                                    MiruInvertedIndex aggregateInvertedIndex = aggregateField.getOrCreateInvertedIndex(aggregateFieldValue);
                                    for (MiruTermId fieldValue : fieldValues) {
                                        MiruTermId compositeAggregateId = makeComposite(fieldValue, "^", aggregateFieldName);
                                        MiruInvertedIndex invertedIndex = miruField.getOrCreateInvertedIndex(compositeAggregateId);

                                        invertedIndex.andNotToSourceSize(aggregateInvertedIndex.getIndex());
                                        miruField.index(compositeAggregateId, id);
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    private MiruTermId makeComposite(MiruTermId fieldValue, String separator, String fieldName) {
        return new MiruTermId(Bytes.concat(fieldValue.getBytes(), separator.getBytes(), fieldName.getBytes()));
    }
}
