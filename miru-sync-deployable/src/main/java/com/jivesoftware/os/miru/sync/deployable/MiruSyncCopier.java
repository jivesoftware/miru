package com.jivesoftware.os.miru.sync.deployable;

import com.google.common.collect.Lists;
import com.jivesoftware.os.miru.api.activity.MiruPartitionId;
import com.jivesoftware.os.miru.api.activity.MiruPartitionedActivity;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.api.wal.MiruCursor;
import com.jivesoftware.os.miru.api.wal.MiruSipCursor;
import com.jivesoftware.os.miru.api.wal.MiruWALClient;
import com.jivesoftware.os.miru.api.wal.MiruWALClient.StreamBatch;
import com.jivesoftware.os.miru.api.wal.MiruWALEntry;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import java.util.List;

/**
 *
 */
public class MiruSyncCopier<C extends MiruCursor<C, S>, S extends MiruSipCursor<S>> {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    private final MiruWALClient<C, S> walClient;
    private final int batchSize;
    private final C defaultCursor;
    private final Class<C> cursorClass;

    public MiruSyncCopier(MiruWALClient<C, S> walClient, int batchSize, C defaultCursor, Class<C> cursorClass) {
        this.walClient = walClient;
        this.batchSize = batchSize;
        this.defaultCursor = defaultCursor;
        this.cursorClass = cursorClass;
    }

    public int copyLocal(MiruTenantId fromTenantId,
        MiruPartitionId fromPartitionId,
        MiruTenantId toTenantId,
        MiruPartitionId toPartitionId,
        long fromTimestamp) throws Exception {

        C cursor = defaultCursor;
        int copied = 0;
        boolean started = false;
        while (true) {
            StreamBatch<MiruWALEntry, C> batch = walClient.getActivity(fromTenantId, fromPartitionId, cursor, batchSize, -1, null);
            int activityTypes = 0;
            if (batch.activities != null && !batch.activities.isEmpty()) {
                List<MiruPartitionedActivity> copyOfActivities = Lists.newArrayListWithCapacity(batch.activities.size());
                for (MiruWALEntry entry : batch.activities) {
                    MiruPartitionedActivity activity = entry.activity;
                    if (activity.type.isActivityType()) {
                        activityTypes++;
                        if (activity.timestamp > fromTimestamp) {
                            started = true;
                        }
                    }

                    if (started) {
                        // hacky use of fromJson()
                        copyOfActivities.add(MiruPartitionedActivity.fromJson(activity.type,
                            activity.writerId,
                            toPartitionId.getId(),
                            toTenantId.getBytes(),
                            activity.index,
                            activity.timestamp,
                            activity.clockTimestamp,
                            activity.activity.orNull(),
                            activity.readEvent.orNull()));
                    }
                }
                if (!copyOfActivities.isEmpty()) {
                    walClient.writeActivity(toTenantId, toPartitionId, copyOfActivities);
                    copied += copyOfActivities.size();
                }
                cursor = batch.cursor;
            }
            if (activityTypes == 0) {
                break;
            }
        }
        return copied;
    }
}
