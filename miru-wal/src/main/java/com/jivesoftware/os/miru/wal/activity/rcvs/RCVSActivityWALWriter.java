package com.jivesoftware.os.miru.wal.activity.rcvs;

import com.jivesoftware.os.miru.api.activity.MiruPartitionId;
import com.jivesoftware.os.miru.api.activity.MiruPartitionedActivity;
import com.jivesoftware.os.miru.api.activity.TenantAndPartition;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.api.topology.RangeMinMax;
import com.jivesoftware.os.miru.wal.activity.MiruActivityWALWriter;
import com.jivesoftware.os.rcvs.api.MultiAdd;
import com.jivesoftware.os.rcvs.api.RowColumValueTimestampAdd;
import com.jivesoftware.os.rcvs.api.RowColumnValueStore;
import com.jivesoftware.os.rcvs.api.timestamper.ConstantTimestamper;
import com.jivesoftware.os.rcvs.api.timestamper.Timestamper;
import java.util.Collection;
import java.util.List;
import java.util.Set;

/**
 * @author jonathan
 */
public class RCVSActivityWALWriter implements MiruActivityWALWriter {

    private final RowColumnValueStore<MiruTenantId, MiruActivityWALRow, MiruActivityWALColumnKey, MiruPartitionedActivity, ? extends Exception> activityWAL;
    private final RowColumnValueStore<MiruTenantId, MiruActivityWALRow, MiruActivitySipWALColumnKey, MiruPartitionedActivity, ? extends Exception> sipWAL;
    private final Set<TenantAndPartition> blacklist;

    public RCVSActivityWALWriter(
        RowColumnValueStore<MiruTenantId, MiruActivityWALRow, MiruActivityWALColumnKey, MiruPartitionedActivity, ? extends Exception> activityWAL,
        RowColumnValueStore<MiruTenantId, MiruActivityWALRow, MiruActivitySipWALColumnKey, MiruPartitionedActivity, ? extends Exception> sipWAL,
        Set<TenantAndPartition> blacklist) {
        this.activityWAL = activityWAL;
        this.sipWAL = sipWAL;
        this.blacklist = blacklist;
    }

    @Override
    public RangeMinMax write(MiruTenantId tenantId, MiruPartitionId partitionId, List<MiruPartitionedActivity> partitionedActivities) throws Exception {

        if (blacklist != null && blacklist.contains(new TenantAndPartition(tenantId, partitionId))) {
            return new RangeMinMax();
        }

        RangeMinMax partitionMinMax = writeActivity(tenantId, partitionId, partitionedActivities);
        writeSip(tenantId, partitionId, partitionedActivities);
        return partitionMinMax;
    }

    @Override
    public void delete(MiruTenantId tenantId, MiruPartitionId partitionId, Collection<MiruActivityWALColumnKey> keys) throws Exception {

        if (blacklist != null && blacklist.contains(new TenantAndPartition(tenantId, partitionId))) {
            return;
        }

        MiruActivityWALColumnKey[] remove = keys.toArray(new MiruActivityWALColumnKey[keys.size()]);
        activityWAL.multiRemove(tenantId, new MiruActivityWALRow(partitionId.getId()), remove, null);
    }

    @Override
    public void deleteSip(MiruTenantId tenantId, MiruPartitionId partitionId, Collection<MiruActivitySipWALColumnKey> keys) throws Exception {

        if (blacklist != null && blacklist.contains(new TenantAndPartition(tenantId, partitionId))) {
            return;
        }

        MiruActivitySipWALColumnKey[] remove = keys.toArray(new MiruActivitySipWALColumnKey[keys.size()]);
        sipWAL.multiRemove(tenantId, new MiruActivityWALRow(partitionId.getId()), remove, null);
    }

    @Override
    public void removePartition(MiruTenantId tenantId, MiruPartitionId partitionId) throws Exception {

        if (blacklist != null && blacklist.contains(new TenantAndPartition(tenantId, partitionId))) {
            return;
        }

        activityWAL.removeRow(tenantId, new MiruActivityWALRow(partitionId.getId()), null);
        sipWAL.removeRow(tenantId, new MiruActivityWALRow(partitionId.getId()), null);
    }

    private RangeMinMax writeActivity(MiruTenantId tenantId,
        MiruPartitionId partitionId,
        List<MiruPartitionedActivity> partitionedActivities) throws Exception {

        MultiAdd<MiruActivityWALRow, MiruActivityWALColumnKey, MiruPartitionedActivity> rawActivities = new MultiAdd<>();
        RangeMinMax rangeMinMax = new RangeMinMax();
        for (MiruPartitionedActivity partitionedActivity : partitionedActivities) {
            if (!partitionedActivity.partitionId.equals(partitionId)) {
                continue;
            }

            if (partitionedActivity.type.isActivityType()) {
                rangeMinMax.put(partitionedActivity.clockTimestamp, partitionedActivity.timestamp);
            }

            long activityCollisionId;
            if (partitionedActivity.type != MiruPartitionedActivity.Type.BEGIN && partitionedActivity.type != MiruPartitionedActivity.Type.END) {
                activityCollisionId = partitionedActivity.timestamp;
            } else {
                activityCollisionId = partitionedActivity.writerId;
            }

            Timestamper timestamper = partitionedActivity.activity.isPresent()
                ? new ConstantTimestamper(partitionedActivity.activity.get().version)
                : null;

            rawActivities.add(
                new MiruActivityWALRow(partitionedActivity.partitionId.getId()),
                new MiruActivityWALColumnKey(partitionedActivity.type.getSort(), activityCollisionId),
                partitionedActivity,
                timestamper);
        }

        List<RowColumValueTimestampAdd<MiruActivityWALRow, MiruActivityWALColumnKey, MiruPartitionedActivity>> tookActivities = rawActivities.take();
        activityWAL.multiRowsMultiAdd(tenantId, tookActivities);
        return rangeMinMax;
    }

    private void writeSip(MiruTenantId tenantId, MiruPartitionId partitionId, List<MiruPartitionedActivity> partitionedActivities) throws Exception {
        MultiAdd<MiruActivityWALRow, MiruActivitySipWALColumnKey, MiruPartitionedActivity> rawSips = new MultiAdd<>();
        for (MiruPartitionedActivity partitionedActivity : partitionedActivities) {
            if (!partitionedActivity.partitionId.equals(partitionId)) {
                continue;
            }

            long sipCollisionId;
            if (partitionedActivity.type != MiruPartitionedActivity.Type.BEGIN && partitionedActivity.type != MiruPartitionedActivity.Type.END) {
                sipCollisionId = partitionedActivity.clockTimestamp;
            } else {
                sipCollisionId = partitionedActivity.writerId;
            }

            rawSips.add(
                new MiruActivityWALRow(partitionedActivity.partitionId.getId()),
                new MiruActivitySipWALColumnKey(partitionedActivity.type.getSort(), sipCollisionId, partitionedActivity.timestamp),
                partitionedActivity,
                null);
        }

        List<RowColumValueTimestampAdd<MiruActivityWALRow, MiruActivitySipWALColumnKey, MiruPartitionedActivity>> tookSips = rawSips.take();
        sipWAL.multiRowsMultiAdd(tenantId, tookSips);
    }

}
