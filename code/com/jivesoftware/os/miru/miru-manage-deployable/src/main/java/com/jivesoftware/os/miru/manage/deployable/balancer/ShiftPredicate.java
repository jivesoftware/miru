package com.jivesoftware.os.miru.manage.deployable.balancer;

import com.jivesoftware.os.miru.api.MiruPartition;
import com.jivesoftware.os.miru.api.activity.MiruPartitionId;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.api.topology.HostHeartbeat;
import java.util.Collection;
import java.util.List;

/**
*
*/
public interface ShiftPredicate {
    boolean needsToShift(MiruTenantId tenantId,
        MiruPartitionId partitionId,
        Collection<HostHeartbeat> hostHeartbeats,
        List<MiruPartition> partitions);
}
