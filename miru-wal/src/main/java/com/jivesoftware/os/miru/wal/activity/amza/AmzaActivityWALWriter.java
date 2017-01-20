package com.jivesoftware.os.miru.wal.activity.amza;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Function;
import com.jivesoftware.os.amza.api.FailedToAchieveQuorumException;
import com.jivesoftware.os.amza.api.partition.Consistency;
import com.jivesoftware.os.amza.service.EmbeddedClientProvider.EmbeddedClient;
import com.jivesoftware.os.amza.service.PartitionIsDisposedException;
import com.jivesoftware.os.amza.service.PropertiesNotPresentException;
import com.jivesoftware.os.miru.api.activity.MiruPartitionId;
import com.jivesoftware.os.miru.api.activity.MiruPartitionedActivity;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.api.marshall.JacksonJsonObjectTypeMarshaller;
import com.jivesoftware.os.miru.api.topology.RangeMinMax;
import com.jivesoftware.os.miru.wal.AmzaWALUtil;
import com.jivesoftware.os.miru.wal.MiruWALWrongRouteException;
import com.jivesoftware.os.miru.wal.activity.MiruActivityWALWriter;
import com.jivesoftware.os.miru.wal.activity.rcvs.MiruActivitySipWALColumnKey;
import com.jivesoftware.os.miru.wal.activity.rcvs.MiruActivityWALColumnKey;
import com.jivesoftware.os.miru.wal.activity.rcvs.MiruActivityWALColumnKeyMarshaller;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 *
 */
public class AmzaActivityWALWriter implements MiruActivityWALWriter {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    private final AmzaWALUtil amzaWALUtil;
    private final long replicateTimeoutMillis;
    private final MiruActivityWALColumnKeyMarshaller columnKeyMarshaller = new MiruActivityWALColumnKeyMarshaller();
    private final Function<MiruPartitionedActivity, byte[]> activityWALKeyFunction;
    private final Function<MiruPartitionedActivity, byte[]> activitySerializerFunction;

    public AmzaActivityWALWriter(AmzaWALUtil amzaWALUtil,
        long replicateTimeoutMillis,
        ObjectMapper mapper) {
        this.amzaWALUtil = amzaWALUtil;
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

        RangeMinMax partitionMinMax = new RangeMinMax();
        EmbeddedClient client = amzaWALUtil.getActivityClient(tenantId, partitionId);
        try {
            if (!partitionedActivities.isEmpty()) {
                // gather before we write because the list will be cleared
                for (MiruPartitionedActivity activity : partitionedActivities) {
                    if (partitionId.equals(activity.partitionId) && activity.type.isActivityType()) {
                        partitionMinMax.put(activity.clockTimestamp, activity.timestamp);
                    }
                }

                client.commit(Consistency.leader_quorum, null,
                    (txKeyValueStream) -> {
                        if (partitionedActivities.isEmpty()) {
                            throw new IllegalStateException("This callback is not reentrant, please fix!");
                        }
                        for (MiruPartitionedActivity activity : partitionedActivities) {
                            long timestamp = activity.activity.isPresent() ? activity.activity.get().version : System.currentTimeMillis();
                            if (!txKeyValueStream.commit(activityWALKeyFunction.apply(activity), activitySerializerFunction.apply(activity), timestamp,
                                false)) {
                                return false;
                            }
                        }
                        // this is only safe because this is a single threaded leader write and this collection is not needed after the commit
                        partitionedActivities.clear();
                        return true;
                    },
                    replicateTimeoutMillis,
                    TimeUnit.MILLISECONDS);
            }

        } catch (PartitionIsDisposedException e) {
            // Ignored
        } catch (FailedToAchieveQuorumException e) {
            throw new MiruWALWrongRouteException(e);
        }
        return partitionMinMax;
    }

    @Override
    public void delete(MiruTenantId tenantId, MiruPartitionId partitionId, Collection<MiruActivityWALColumnKey> keys) throws Exception {
        EmbeddedClient client = amzaWALUtil.getActivityClient(tenantId, partitionId);
        if (client != null) {
            try {
                client.commit(Consistency.leader_quorum,
                    null,
                    (txKeyValueStream) -> {
                        for (MiruActivityWALColumnKey columnKey : keys) {
                            if (!txKeyValueStream.commit(columnKeyMarshaller.toLexBytes(columnKey), null, -1, true)) {
                                return false;
                            }
                        }
                        return true;
                    },
                    replicateTimeoutMillis,
                    TimeUnit.MILLISECONDS);
            } catch (PropertiesNotPresentException | PartitionIsDisposedException e) {
                // Ignored
            } catch (FailedToAchieveQuorumException e) {
                throw new MiruWALWrongRouteException(e);
            }
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
}
