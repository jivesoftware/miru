package com.jivesoftware.os.miru.sync.deployable;

import com.jivesoftware.os.miru.api.activity.MiruPartitionId;
import com.jivesoftware.os.miru.api.activity.MiruPartitionedActivity;
import com.jivesoftware.os.miru.api.activity.schema.MiruSchema;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.api.sync.MiruSyncClient;
import com.jivesoftware.os.miru.api.wal.MiruWALClient;
import java.util.List;

/**
 *
 */
public class LoopbackSyncClient implements MiruSyncClient {

    private final MiruWALClient<?, ?> walClient;

    public LoopbackSyncClient(MiruWALClient<?, ?> walClient) {
        this.walClient = walClient;
    }

    @Override
    public void writeActivity(MiruTenantId tenantId, MiruPartitionId partitionId, List<MiruPartitionedActivity> partitionedActivities) throws Exception {
        walClient.writeActivity(tenantId, partitionId, partitionedActivities);
    }

    @Override
    public void registerSchema(MiruTenantId tenantId, MiruSchema schema) throws Exception {
        // no op
    }
}
