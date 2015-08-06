package com.jivesoftware.os.miru.wal.activity.amza;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Function;
import com.jivesoftware.os.amza.client.AmzaClientProvider;
import com.jivesoftware.os.amza.shared.AmzaPartitionUpdates;
import com.jivesoftware.os.miru.api.activity.MiruPartitionId;
import com.jivesoftware.os.miru.api.activity.MiruPartitionedActivity;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.api.marshall.JacksonJsonObjectTypeMarshaller;
import com.jivesoftware.os.miru.api.topology.RangeMinMax;
import com.jivesoftware.os.miru.wal.AmzaWALUtil;
import com.jivesoftware.os.miru.wal.activity.MiruActivityWALWriter;
import com.jivesoftware.os.miru.wal.activity.rcvs.MiruActivitySipWALColumnKey;
import com.jivesoftware.os.miru.wal.activity.rcvs.MiruActivityWALColumnKey;
import com.jivesoftware.os.miru.wal.activity.rcvs.MiruActivityWALColumnKeyMarshaller;
import java.util.Collection;
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
    private final Function<MiruPartitionedActivity, byte[]> activityWALKeyFunction;
    private final Function<MiruPartitionedActivity, byte[]> activitySerializerFunction;

    public AmzaActivityWALWriter(AmzaWALUtil amzaWALUtil,
        int replicateRequireNReplicas,
        long replicateTimeoutMillis,
        ObjectMapper mapper) {
        this.amzaWALUtil = amzaWALUtil;
        this.replicateRequireNReplicas = replicateRequireNReplicas;
        this.replicateTimeoutMillis = replicateTimeoutMillis;

        JacksonJsonObjectTypeMarshaller<MiruPartitionedActivity> partitionedActivityMarshaller =
            new JacksonJsonObjectTypeMarshaller<>(MiruPartitionedActivity.class, mapper);
        this.activityWALKeyFunction = (partitionedActivity) -> {
            long activityCollisionId;
            if (partitionedActivity.type != MiruPartitionedActivity.Type.BEGIN && partitionedActivity.type != MiruPartitionedActivity.Type.END) {
                activityCollisionId = partitionedActivity.timestamp;
            } else {
                activityCollisionId = partitionedActivity.writerId;
            }

            try {
                return columnKeyMarshaller.toLexBytes(new MiruActivityWALColumnKey(partitionedActivity.type.getSort(), activityCollisionId));
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        };
        this.activitySerializerFunction = partitionedActivity -> {
            try {
                return partitionedActivityMarshaller.toBytes(partitionedActivity);
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

        amzaWALUtil.getActivityClient(tenantId, partitionId).commit(null,
            (highwaters, txKeyValueStream) -> {
                for (MiruPartitionedActivity activity : partitionedActivities) {
                    long timestamp = activity.activity.isPresent() ? activity.activity.get().version : System.currentTimeMillis();
                    if (!txKeyValueStream.row(-1, activityWALKeyFunction.apply(activity), activitySerializerFunction.apply(activity), timestamp, false)) {
                        return false;
                    }
                }
                return true;
            },
            replicateRequireNReplicas,
            replicateTimeoutMillis,
            TimeUnit.MILLISECONDS);

        for (MiruPartitionedActivity activity : partitionedActivities) {
            if (partitionId.equals(activity.partitionId)) {
                partitionMinMax.put(activity.clockTimestamp, activity.timestamp);
            }
        }

        return partitionMinMax;
    }

    @Override
    public void delete(MiruTenantId tenantId, MiruPartitionId partitionId, Collection<MiruActivityWALColumnKey> keys) throws Exception {
        AmzaClientProvider.AmzaClient client = amzaWALUtil.getActivityClient(tenantId, partitionId);
        if (client != null) {
            client.commit(null, (highwaters, txKeyValueStream) -> {
                    for (MiruActivityWALColumnKey columnKey : keys) {
                        if (!txKeyValueStream.row(-1, columnKeyMarshaller.toLexBytes(columnKey), null, -1, true)) {
                            return false;
                        }
                    }
                    return true;
                },
                replicateRequireNReplicas,
                replicateTimeoutMillis,
                TimeUnit.MILLISECONDS);
        }
    }

    @Override
    public void deleteSip(MiruTenantId tenantId, MiruPartitionId partitionId, Collection<MiruActivitySipWALColumnKey> keys) throws Exception {
        // no such thing
    }

    @Override
    public void removePartition(MiruTenantId tenantId, MiruPartitionId partitionId) throws Exception {
        amzaWALUtil.destroyActivityPartition(tenantId, partitionId);
    }

    private void recordTenantPartition(MiruTenantId tenantId, MiruPartitionId partitionId) throws Exception {
        AmzaPartitionUpdates updates = new AmzaPartitionUpdates().set(amzaWALUtil.toPartitionsKey(tenantId, partitionId), null);
        amzaWALUtil.getLookupPartitionsClient().commit(null, updates, 0, 0, TimeUnit.MILLISECONDS);
    }
}
