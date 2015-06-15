package com.jivesoftware.os.miru.wal.activity.amza;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Function;
import com.jivesoftware.os.amza.shared.AmzaPartitionUpdates;
import com.jivesoftware.os.amza.shared.scan.Commitable;
import com.jivesoftware.os.amza.shared.scan.Scan;
import com.jivesoftware.os.amza.shared.take.Highwaters;
import com.jivesoftware.os.amza.shared.wal.WALKey;
import com.jivesoftware.os.amza.shared.wal.WALValue;
import com.jivesoftware.os.miru.api.activity.MiruPartitionId;
import com.jivesoftware.os.miru.api.activity.MiruPartitionedActivity;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.api.marshall.JacksonJsonObjectTypeMarshaller;
import com.jivesoftware.os.miru.api.topology.RangeMinMax;
import com.jivesoftware.os.miru.wal.AmzaWALUtil;
import com.jivesoftware.os.miru.wal.activity.MiruActivityWALWriter;
import com.jivesoftware.os.miru.wal.activity.rcvs.MiruActivityWALColumnKey;
import com.jivesoftware.os.miru.wal.activity.rcvs.MiruActivityWALColumnKeyMarshaller;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 *
 */
public class AmzaActivityWALWriter implements MiruActivityWALWriter {

    private final AmzaWALUtil amzaWALUtil;
    private final int replicateRequireNReplicas;
    private final long replicateTimeoutMillis;
    private final MiruActivityWALColumnKeyMarshaller columnKeyMarshaller = new MiruActivityWALColumnKeyMarshaller();
    private final JacksonJsonObjectTypeMarshaller<MiruPartitionedActivity> partitionedActivityMarshaller;
    private final Function<MiruPartitionedActivity, WALKey> activityWALKeyFunction;
    private final Function<MiruPartitionedActivity, WALValue> activitySerializerFunction;

    public AmzaActivityWALWriter(AmzaWALUtil amzaWALUtil,
        int replicateRequireNReplicas,
        long replicateTimeoutMillis,
        ObjectMapper mapper) {
        this.amzaWALUtil = amzaWALUtil;
        this.replicateRequireNReplicas = replicateRequireNReplicas;
        this.replicateTimeoutMillis = replicateTimeoutMillis;

        this.partitionedActivityMarshaller = new JacksonJsonObjectTypeMarshaller<>(MiruPartitionedActivity.class, mapper);
        this.activityWALKeyFunction = (partitionedActivity) -> {
            long activityCollisionId;
            if (partitionedActivity.type != MiruPartitionedActivity.Type.BEGIN && partitionedActivity.type != MiruPartitionedActivity.Type.END) {
                activityCollisionId = partitionedActivity.timestamp;
            } else {
                activityCollisionId = partitionedActivity.writerId;
            }

            try {
                return new WALKey(columnKeyMarshaller.toLexBytes(new MiruActivityWALColumnKey(partitionedActivity.type.getSort(),
                    activityCollisionId)));
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        };
        this.activitySerializerFunction = partitionedActivity -> {
            try {
                long timestamp = partitionedActivity.activity.isPresent()
                    ? partitionedActivity.activity.get().version
                    : System.currentTimeMillis();
                return new WALValue(partitionedActivityMarshaller.toBytes(partitionedActivity), timestamp, false);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        };
    }

    @Override
    public RangeMinMax write(MiruTenantId tenantId,
        MiruPartitionId partitionId,
        List<MiruPartitionedActivity> partitionedActivities) throws Exception {

        recordTenantPartition(tenantId, partitionId);
        RangeMinMax partitionMinMax = new RangeMinMax();
        for (MiruPartitionedActivity activity : partitionedActivities) {
            if (partitionId.equals(activity.partitionId)) {
                partitionMinMax.put(activity.clockTimestamp, activity.timestamp);
            }
        }

        amzaWALUtil.getActivityClient(tenantId, partitionId).commit(
            new ActivityUpdates(partitionId, partitionedActivities, activityWALKeyFunction, activitySerializerFunction),
            replicateRequireNReplicas,
            replicateTimeoutMillis,
            TimeUnit.MILLISECONDS);

        return partitionMinMax;
    }

    @Override
    public void removePartition(MiruTenantId tenantId, MiruPartitionId partitionId) throws Exception {
        amzaWALUtil.destroyActivityPartition(tenantId, partitionId);
    }

    private void recordTenantPartition(MiruTenantId tenantId, MiruPartitionId partitionId) throws Exception {
        AmzaPartitionUpdates updates = new AmzaPartitionUpdates().set(amzaWALUtil.toPartitionsKey(tenantId, partitionId), null);
        amzaWALUtil.getLookupPartitionsClient().commit(updates, 0, 0, TimeUnit.MILLISECONDS);
    }

    private static class ActivityUpdates implements Commitable<WALValue> {

        private final MiruPartitionId partitionId;
        private final List<MiruPartitionedActivity> activities;
        private final Function<MiruPartitionedActivity, WALKey> walKeyFunction;
        private final Function<MiruPartitionedActivity, WALValue> walValueFunction;

        public ActivityUpdates(MiruPartitionId partitionId,
            List<MiruPartitionedActivity> activities,
            Function<MiruPartitionedActivity, WALKey> walKeyFunction,
            Function<MiruPartitionedActivity, WALValue> walValueFunction) {
            this.partitionId = partitionId;
            this.activities = activities;
            this.walKeyFunction = walKeyFunction;
            this.walValueFunction = walValueFunction;
        }

        @Override
        public void commitable(Highwaters highwaters, Scan<WALValue> scan) throws Exception {
            for (MiruPartitionedActivity activity : activities) {
                if (partitionId.equals(activity.partitionId)) {
                    if (!scan.row(-1, walKeyFunction.apply(activity), walValueFunction.apply(activity))) {
                        return;
                    }
                }
            }
        }
    }
}
