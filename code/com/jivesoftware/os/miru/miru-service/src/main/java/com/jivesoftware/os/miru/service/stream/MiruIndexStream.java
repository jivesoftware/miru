package com.jivesoftware.os.miru.service.stream;

import com.google.common.collect.Sets;
import com.jivesoftware.os.jive.utils.base.util.locks.StripingLocksProvider;
import com.jivesoftware.os.jive.utils.logger.MetricLogger;
import com.jivesoftware.os.jive.utils.logger.MetricLoggerFactory;
import com.jivesoftware.os.miru.api.activity.MiruActivity;
import com.jivesoftware.os.miru.api.base.MiruTermId;
import com.jivesoftware.os.miru.service.index.MiruActivityIndex;
import com.jivesoftware.os.miru.service.index.MiruAuthzIndex;
import com.jivesoftware.os.miru.service.index.MiruField;
import com.jivesoftware.os.miru.service.index.MiruFields;
import com.jivesoftware.os.miru.service.index.MiruRemovalIndex;
import com.jivesoftware.os.miru.service.schema.MiruSchema;
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
                MiruField termIndex = fieldIndex.getField(fieldId);
                for (MiruTermId term : activity.fieldsValues.get(fieldName)) {
                    termIndex.index(term, id);
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
}
