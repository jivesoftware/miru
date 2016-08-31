package com.jivesoftware.os.miru.reco.plugins;

import com.google.common.collect.Maps;
import com.jivesoftware.os.miru.api.activity.MiruActivity;
import com.jivesoftware.os.miru.api.activity.MiruPartitionId;
import com.jivesoftware.os.miru.api.activity.MiruPartitionedActivity;
import com.jivesoftware.os.miru.api.activity.MiruPartitionedActivityFactory;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 *
 */
public class CollaborativeFilterUtil {

    private final MiruPartitionedActivityFactory partitionedActivityFactory = new MiruPartitionedActivityFactory();
    private static final String[] animals = new String[] { "cat", "dog", "elephant", "aardvark", "camel", "moose", "mouse", "rabbit", "dingo", "eel" };

    public MiruPartitionedActivity viewActivity(MiruTenantId tenantId,
        MiruPartitionId partitionId,
        long time,
        String user,
        String doc,
        int index) {

        int hash = Math.abs(doc.hashCode());
        Map<String, List<String>> fieldsValues = Maps.newHashMap();
        fieldsValues.put("user", Arrays.asList(user));
        fieldsValues.put("doc", Arrays.asList(doc));
        fieldsValues.put("obj", Arrays.asList((hash % 4) + " " + doc));
        fieldsValues.put("text", Arrays.asList(
            animals[hash % animals.length], animals[Math.abs((hash * 3) % animals.length)], animals[Math.abs((hash * 7) % animals.length)]));

        MiruActivity activity = new MiruActivity(tenantId, time, 0, false, new String[0], fieldsValues, Collections.emptyMap());
        return partitionedActivityFactory.activity(1, partitionId, index, activity);
    }

    public MiruPartitionedActivity typedActivity(MiruTenantId tenantId,
        MiruPartitionId partitionId,
        long time,
        String user,
        String doc,
        String activityType,
        int index) {

        int hash = Math.abs(doc.hashCode());
        Map<String, List<String>> fieldsValues = Maps.newHashMap();
        fieldsValues.put("user", Arrays.asList(user));
        fieldsValues.put("doc", Arrays.asList(doc));
        fieldsValues.put("docType", Arrays.asList(String.valueOf(hash % 4)));
        fieldsValues.put("activityType", Arrays.asList(activityType));

        MiruActivity activity = new MiruActivity(tenantId, time, 0, false, new String[0], fieldsValues, Collections.emptyMap());
        return partitionedActivityFactory.activity(1, partitionId, index, activity);
    }
}
