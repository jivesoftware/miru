package com.jivesoftware.os.miru.api.sync;

import com.jivesoftware.os.miru.api.activity.MiruPartitionId;
import com.jivesoftware.os.miru.api.activity.MiruPartitionedActivity;
import com.jivesoftware.os.miru.api.activity.schema.MiruSchema;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import java.util.List;

/**
 * @author jonathan.colt
 */
public interface MiruSyncClient {

    void writeActivity(MiruTenantId tenantId, MiruPartitionId partitionId, List<MiruPartitionedActivity> partitionedActivities) throws Exception;

    void registerSchema(MiruTenantId tenantId, MiruSchema schema) throws Exception;
}
