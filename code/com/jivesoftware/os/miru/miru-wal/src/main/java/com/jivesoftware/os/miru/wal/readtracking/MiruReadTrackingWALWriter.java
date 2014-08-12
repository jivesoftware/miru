package com.jivesoftware.os.miru.wal.readtracking;

import com.jivesoftware.os.miru.api.activity.MiruPartitionedActivity;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import java.util.List;

public interface MiruReadTrackingWALWriter {

    public void write(MiruTenantId tenantId, List<MiruPartitionedActivity> partitionedActivities) throws Exception;

}
