package com.jivesoftware.os.miru.wal.activity.amza;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimaps;
import com.jivesoftware.os.amza.service.AmzaRegion;
import com.jivesoftware.os.amza.shared.MemoryWALUpdates;
import com.jivesoftware.os.amza.shared.RegionName;
import com.jivesoftware.os.amza.shared.RegionProperties;
import com.jivesoftware.os.amza.shared.WALKey;
import com.jivesoftware.os.amza.shared.WALValue;
import com.jivesoftware.os.miru.api.activity.MiruPartitionId;
import com.jivesoftware.os.miru.api.activity.MiruPartitionedActivity;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.api.marshall.JacksonJsonObjectTypeMarshaller;
import com.jivesoftware.os.miru.wal.AmzaWALUtil;
import com.jivesoftware.os.miru.wal.activity.MiruActivityWALWriter;
import com.jivesoftware.os.miru.wal.activity.rcvs.MiruActivityWALColumnKey;
import com.jivesoftware.os.miru.wal.activity.rcvs.MiruActivityWALColumnKeyMarshaller;
import java.util.AbstractMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.TreeMap;

/**
 *
 */
public class AmzaActivityWALWriter implements MiruActivityWALWriter {

    private final AmzaWALUtil amzaWALUtil;
    private final int ringSize;
    private final int replicateRequireNReplicas;
    private final MiruActivityWALColumnKeyMarshaller columnKeyMarshaller = new MiruActivityWALColumnKeyMarshaller();
    private final JacksonJsonObjectTypeMarshaller<MiruPartitionedActivity> partitionedActivityMarshaller;
    private final Function<MiruPartitionedActivity, WALKey> activityWALKeyFunction;
    private final Function<MiruPartitionedActivity, WALValue> activitySerializerFunction;

    public AmzaActivityWALWriter(AmzaWALUtil amzaWALUtil,
        int ringSize,
        int replicateRequireNReplicas,
        ObjectMapper mapper) {
        this.amzaWALUtil = amzaWALUtil;
        this.ringSize = ringSize;
        this.replicateRequireNReplicas = replicateRequireNReplicas;

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
    public void write(MiruTenantId tenantId, List<MiruPartitionedActivity> partitionedActivities) throws Exception {
        ListMultimap<MiruPartitionId, MiruPartitionedActivity> partitions = Multimaps.index(partitionedActivities, input -> input.partitionId);
        Set<MiruPartitionId> partitionIds = partitions.keySet();
        recordTenantPartitions(tenantId, partitionIds);
        for (MiruPartitionId partitionId : partitionIds) {
            List<MiruPartitionedActivity> activities = partitions.get(partitionId);
            NavigableMap<WALKey, WALValue> activityMap = buildMap(activities, activityWALKeyFunction, activitySerializerFunction);

            String walName = "activityWAL-" + tenantId.toString() + "-" + partitionId.toString();
            RegionName activityRegionName = new RegionName(false, walName, walName);
            amzaWALUtil.getOrCreateRegion(activityRegionName, ringSize, Optional.<RegionProperties>absent());

            if (!amzaWALUtil.replicate(activityRegionName, new MemoryWALUpdates(activityMap, null), replicateRequireNReplicas)) {
                throw new RuntimeException("Failed to write to amza activity WAL because it's under-replicated for tenant:" + tenantId +
                    " partition:" + partitionId);
            }
        }
    }

    private void recordTenantPartitions(MiruTenantId tenantId, Set<MiruPartitionId> partitionIds) throws Exception {
        List<Map.Entry<WALKey, WALValue>> partitionEntries = Lists.newArrayListWithCapacity(partitionIds.size());
        for (MiruPartitionId partitionId : partitionIds) {
            partitionEntries.add(new AbstractMap.SimpleEntry<>(amzaWALUtil.toPartitionsKey(tenantId, partitionId), AmzaWALUtil.NULL_VALUE));
        }

        AmzaRegion partitionsRegion = amzaWALUtil.getOrCreateMaximalRegion(AmzaWALUtil.LOOKUP_PARTITIONS_REGION_NAME, Optional.<RegionProperties>absent());
        partitionsRegion.setValues(partitionEntries);
    }

    @Override
    public void removePartition(MiruTenantId tenantId, MiruPartitionId partitionId) throws Exception {
        /*TODO
        String ringName = "activityWAL-" + tenantId.toString() + "-" + partitionId.toString();
        RegionName activityRegionName = new RegionName(false, ringName, "activityWAL-" + tenantId.toString() + "-" + partitionId.toString());
        amzaWALUtil.destroyRegion(activityRegionName);
        */
        throw new UnsupportedOperationException("Doesn't work yet");
    }

    private NavigableMap<WALKey, WALValue> buildMap(List<MiruPartitionedActivity> activities,
        Function<MiruPartitionedActivity, WALKey> walKeyFunction,
        Function<MiruPartitionedActivity, WALValue> walValueFunction)
        throws Exception {
        NavigableMap<WALKey, WALValue> activityMap = new TreeMap<>();
        for (MiruPartitionedActivity partitionedActivity : activities) {
            activityMap.put(walKeyFunction.apply(partitionedActivity), walValueFunction.apply(partitionedActivity));
        }
        return activityMap;
    }
}
