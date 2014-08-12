package com.jivesoftware.os.miru.wal.activity;

import com.jivesoftware.os.miru.api.activity.MiruPartitionedActivity;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import java.util.List;

/**
 * @author jonathan
 */
public interface MiruActivityWALWriter {

    void write(MiruTenantId tenantId, List<MiruPartitionedActivity> partitionedActivities) throws Exception;

}
