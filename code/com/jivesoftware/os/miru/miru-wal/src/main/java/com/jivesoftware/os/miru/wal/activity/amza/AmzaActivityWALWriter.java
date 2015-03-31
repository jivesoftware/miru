package com.jivesoftware.os.miru.wal.activity.amza;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Function;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Multimaps;
import com.jivesoftware.os.amza.service.AmzaService;
import com.jivesoftware.os.amza.shared.MemoryWALIndex;
import com.jivesoftware.os.amza.shared.RegionName;
import com.jivesoftware.os.amza.shared.RegionProperties;
import com.jivesoftware.os.amza.shared.WALKey;
import com.jivesoftware.os.amza.shared.WALStorageDescriptor;
import com.jivesoftware.os.amza.shared.WALValue;
import com.jivesoftware.os.miru.api.activity.MiruPartitionId;
import com.jivesoftware.os.miru.api.activity.MiruPartitionedActivity;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.api.marshall.JacksonJsonObjectTypeMarshaller;
import com.jivesoftware.os.miru.wal.activity.MiruActivityWALWriter;
import com.jivesoftware.os.miru.wal.activity.rcvs.MiruActivitySipWALColumnKey;
import com.jivesoftware.os.miru.wal.activity.rcvs.MiruActivitySipWALColumnKeyMarshaller;
import com.jivesoftware.os.miru.wal.activity.rcvs.MiruActivityWALColumnKey;
import com.jivesoftware.os.miru.wal.activity.rcvs.MiruActivityWALColumnKeyMarshaller;
import java.util.List;
import java.util.NavigableMap;
import java.util.TreeMap;

/**
 *
 */
public class AmzaActivityWALWriter implements MiruActivityWALWriter {

    private final AmzaService amzaService;
    private final WALStorageDescriptor activityWALStorageDescriptor;
    private final WALStorageDescriptor sipWALStorageDescriptor;
    private final int replicateToNHosts;
    private final int requireNReplicas;
    private final MiruActivityWALColumnKeyMarshaller activityMarshaller = new MiruActivityWALColumnKeyMarshaller();
    private final MiruActivitySipWALColumnKeyMarshaller sipMarshaller = new MiruActivitySipWALColumnKeyMarshaller();
    private final JacksonJsonObjectTypeMarshaller<MiruPartitionedActivity> partitionedActivityMarshaller;
    private final Function<MiruPartitionedActivity, WALKey> activityWALKeyFunction;
    private final Function<MiruPartitionedActivity, WALKey> sipWALKeyFunction;
    private final Function<MiruPartitionedActivity, WALValue> activitySerializerFunction;
    private final Function<MiruPartitionedActivity, WALValue> sipSerializerFunction;

    public AmzaActivityWALWriter(AmzaService amzaService,
        WALStorageDescriptor activityWALStorageDescriptor,
        WALStorageDescriptor sipWALStorageDescriptor,
        int replicateToNHosts,
        int requireNReplicas,
        ObjectMapper mapper) {
        this.amzaService = amzaService;
        this.activityWALStorageDescriptor = activityWALStorageDescriptor;
        this.sipWALStorageDescriptor = sipWALStorageDescriptor;
        this.replicateToNHosts = replicateToNHosts;
        this.requireNReplicas = requireNReplicas;

        this.partitionedActivityMarshaller = new JacksonJsonObjectTypeMarshaller<>(MiruPartitionedActivity.class, mapper);
        this.activityWALKeyFunction = new Function<MiruPartitionedActivity, WALKey>() {
            @Override
            public WALKey apply(MiruPartitionedActivity partitionedActivity) {
                long activityCollisionId;
                if (partitionedActivity.type != MiruPartitionedActivity.Type.BEGIN && partitionedActivity.type != MiruPartitionedActivity.Type.END) {
                    activityCollisionId = partitionedActivity.timestamp;
                } else {
                    activityCollisionId = partitionedActivity.writerId;
                }

                try {
                    return new WALKey(activityMarshaller.toLexBytes(new MiruActivityWALColumnKey(partitionedActivity.type.getSort(),
                        activityCollisionId)));
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
        };
        this.sipWALKeyFunction = new Function<MiruPartitionedActivity, WALKey>() {
            @Override
            public WALKey apply(MiruPartitionedActivity partitionedActivity) {
                long sipCollisionId;
                if (partitionedActivity.type != MiruPartitionedActivity.Type.BEGIN && partitionedActivity.type != MiruPartitionedActivity.Type.END) {
                    sipCollisionId = partitionedActivity.clockTimestamp;
                } else {
                    sipCollisionId = partitionedActivity.writerId;
                }

                try {
                    return new WALKey(sipMarshaller.toLexBytes(new MiruActivitySipWALColumnKey(partitionedActivity.type.getSort(), sipCollisionId,
                        partitionedActivity.timestamp)));
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
        };
        this.activitySerializerFunction = new Function<MiruPartitionedActivity, WALValue>() {
            @Override
            public WALValue apply(MiruPartitionedActivity partitionedActivity) {
                try {
                    long timestamp = partitionedActivity.activity.isPresent()
                        ? partitionedActivity.activity.get().version
                        : System.currentTimeMillis();
                    return new WALValue(partitionedActivityMarshaller.toBytes(partitionedActivity), timestamp, false);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
        };
        this.sipSerializerFunction = new Function<MiruPartitionedActivity, WALValue>() {
            @Override
            public WALValue apply(MiruPartitionedActivity partitionedActivity) {
                try {
                    return new WALValue(partitionedActivityMarshaller.toBytes(partitionedActivity), System.currentTimeMillis(), false);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
        };
    }

    @Override
    public void write(MiruTenantId tenantId, List<MiruPartitionedActivity> partitionedActivities) throws Exception {
        ListMultimap<MiruPartitionId, MiruPartitionedActivity> partitions = Multimaps.index(partitionedActivities,
            new Function<MiruPartitionedActivity, MiruPartitionId>() {
                @Override
                public MiruPartitionId apply(MiruPartitionedActivity input) {
                    return input.partitionId;
                }
            });

        for (MiruPartitionId partitionId : partitions.keySet()) {
            List<MiruPartitionedActivity> activities = partitions.get(partitionId);
            NavigableMap<WALKey, WALValue> activityMap = buildMap(activities, activityWALKeyFunction, activitySerializerFunction);
            NavigableMap<WALKey, WALValue> sipMap = buildMap(activities, sipWALKeyFunction, sipSerializerFunction);

            String ringName = "activityWAL-" + tenantId.toString() + "-" + partitionId.toString();
            ensureRing(ringName);

            RegionName activityRegionName = new RegionName(false, ringName, "activityWAL-" + tenantId.toString() + "-" + partitionId.toString());
            RegionName sipRegionName = new RegionName(false, ringName, "sipWAL-" + tenantId.toString() + "-" + partitionId.toString());

            amzaService.createRegionIfAbsent(activityRegionName, new RegionProperties(activityWALStorageDescriptor, 1, 1, false));
            amzaService.createRegionIfAbsent(sipRegionName, new RegionProperties(sipWALStorageDescriptor, 1, 1, false));
            if (amzaService.replicate(activityRegionName, new MemoryWALIndex(activityMap), replicateToNHosts, requireNReplicas)) {
                if (amzaService.replicate(sipRegionName, new MemoryWALIndex(sipMap), replicateToNHosts, requireNReplicas)) {
                    // success
                } else {
                    throw new RuntimeException("Failed to write to amza sip WAL because it's under-replicated for tenant:" + tenantId +
                        " partition:" + partitionId);
                }
            } else {
                throw new RuntimeException("Failed to write to amza activity WAL because it's under-replicated for tenant:" + tenantId +
                    " partition:" + partitionId);
            }
        }
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

    private void ensureRing(String ringName) throws Exception {
        if (amzaService.getAmzaRing().getRing(ringName).isEmpty()) {
            amzaService.getAmzaRing().buildRandomSubRing(ringName, 3); //TODO configure replication factor
        }
    }
}
