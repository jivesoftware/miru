package com.jivesoftware.os.miru.manage.deployable;

import com.google.common.collect.Sets;
import com.jivesoftware.os.miru.api.MiruHost;
import com.jivesoftware.os.miru.api.MiruPartition;
import com.jivesoftware.os.miru.api.activity.MiruPartitionId;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.api.topology.HostHeartbeat;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 *
 */
public class UnhealthyTopologyShiftPredicate implements ShiftPredicate {

    private float unhealthyPct;

    public UnhealthyTopologyShiftPredicate(float unhealthyPct) {
        this.unhealthyPct = unhealthyPct;
    }

    @Override
    public boolean needsToShift(MiruTenantId tenantId,
        MiruPartitionId partitionId,
        Collection<HostHeartbeat> hostHeartbeats,
        List<MiruPartition> partitions) {

        Set<MiruHost> unhealthyHosts = Sets.newHashSet();
        for (HostHeartbeat hostHeartbeat : hostHeartbeats) {
            if (hostHeartbeat.heartbeat < System.currentTimeMillis() - TimeUnit.MINUTES.toMillis(1)) { //TODO configure
                unhealthyHosts.add(hostHeartbeat.host);
            }
        }

        int numberOfReplicas = partitions.size();
        int unhealthyCount = 0;
        for (MiruPartition partition : partitions) {
            if (unhealthyHosts.contains(partition.coord.host)) {
                unhealthyCount++;
            }
        }

        float currentPct = unhealthyCount / numberOfReplicas;
        return (currentPct > unhealthyPct);
    }
}
