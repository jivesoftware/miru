package com.jivesoftware.os.miru.sync.deployable;

import com.jivesoftware.os.miru.api.activity.MiruPartitionId;
import com.jivesoftware.os.miru.api.activity.MiruPartitionedActivity;
import com.jivesoftware.os.miru.api.activity.schema.MiruSchema;
import com.jivesoftware.os.miru.api.base.MiruStreamId;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.api.topology.MiruClusterClient;
import com.jivesoftware.os.miru.api.wal.MiruCursor;
import com.jivesoftware.os.miru.api.wal.MiruSipCursor;
import com.jivesoftware.os.miru.api.wal.MiruWALClient;
import java.util.List;

/**
 *
 */
public class MiruSyncReceiver<C extends MiruCursor<C, S>, S extends MiruSipCursor<S>> {

    private final MiruWALClient<C, S> walClient;
    private final MiruClusterClient clusterClient;

    public MiruSyncReceiver(MiruWALClient<C, S> walClient, MiruClusterClient clusterClient) {
        this.walClient = walClient;
        this.clusterClient = clusterClient;
    }

    public void writeActivity(MiruTenantId tenantId, MiruPartitionId partitionId, List<MiruPartitionedActivity> activities) throws Exception {
        walClient.writeActivity(tenantId, partitionId, activities);
    }

    public void writeReadTracking(MiruTenantId tenantId, MiruStreamId streamId, List<MiruPartitionedActivity> activities) throws Exception {
        walClient.writeReadTracking(tenantId, streamId, activities);
    }

    public void registerSchema(MiruTenantId tenantId, MiruSchema schema) throws Exception {
        clusterClient.registerSchema(tenantId, schema);
    }
}
