package com.jivesoftware.os.miru.sync.deployable;

import com.jivesoftware.os.miru.api.activity.MiruPartitionId;
import com.jivesoftware.os.miru.api.activity.MiruPartitionedActivity;
import com.jivesoftware.os.miru.api.base.MiruStreamId;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import java.util.List;
import java.util.Map;

/**
 *
 */
public interface ActivityReadEventConverter {

    Map<MiruStreamId, List<MiruPartitionedActivity>> convert(MiruTenantId tenantId,
        MiruPartitionId partitionId,
        List<MiruPartitionedActivity> activities) throws Exception;
}
