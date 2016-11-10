package com.jivesoftware.os.miru.sync.deployable;

import com.jivesoftware.os.miru.api.activity.MiruPartitionId;
import com.jivesoftware.os.miru.api.activity.MiruPartitionedActivity;
import com.jivesoftware.os.miru.api.activity.schema.MiruSchema;
import com.jivesoftware.os.miru.api.base.MiruStreamId;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.api.sync.ActivityReadEventConverter;
import com.jivesoftware.os.miru.api.topology.MiruClusterClient;
import com.jivesoftware.os.miru.api.wal.MiruCursor;
import com.jivesoftware.os.miru.api.wal.MiruSipCursor;
import com.jivesoftware.os.miru.api.wal.MiruWALClient;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

/**
 *
 */
public class MiruSyncReceiver<C extends MiruCursor<C, S>, S extends MiruSipCursor<S>> {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    private final MiruWALClient<C, S> walClient;
    private final MiruClusterClient clusterClient;
    private final ActivityReadEventConverter converter;

    public MiruSyncReceiver(MiruWALClient<C, S> walClient, MiruClusterClient clusterClient, ActivityReadEventConverter converter) {
        this.walClient = walClient;
        this.clusterClient = clusterClient;
        this.converter = converter;
    }

    public void writeActivity(MiruTenantId tenantId, MiruPartitionId partitionId, List<MiruPartitionedActivity> activities) throws Exception {
        LOG.info("Received from tenantId:{} partitionId:{} activities:{}", tenantId, partitionId, activities.size());
        walClient.writeActivity(tenantId, partitionId, activities);

        if (converter != null) {
            Map<MiruStreamId, List<MiruPartitionedActivity>> converted = converter.convert(tenantId, partitionId, activities);
            if (converted != null) {
                int readCount = 0;
                for (Entry<MiruStreamId, List<MiruPartitionedActivity>> entry : converted.entrySet()) {
                    MiruStreamId streamId = entry.getKey();
                    List<MiruPartitionedActivity> readActivities = entry.getValue();
                    if (!readActivities.isEmpty()) {
                        walClient.writeReadTracking(tenantId, streamId, readActivities);
                        readCount += readActivities.size();
                    }
                }
                LOG.info("Converted from tenantId:{} partitionId:{} readTracks:{}", tenantId, partitionId, readCount);
            }
        }
    }

    public void registerSchema(MiruTenantId tenantId, MiruSchema schema) throws Exception {
        clusterClient.registerSchema(tenantId, schema);
    }
}
