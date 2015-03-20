package com.jivesoftware.os.miru.manage.deployable;

import com.jivesoftware.os.miru.api.MiruPartition;
import com.jivesoftware.os.miru.api.activity.MiruPartitionId;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.api.topology.HostHeartbeat;
import com.jivesoftware.os.miru.manage.deployable.balancer.ShiftPredicate;
import java.util.Collection;
import java.util.List;
import java.util.Random;

/**
 *
 */
public class RandomShiftPredicate implements ShiftPredicate {

    private final float probability;
    private final Random random = new Random();

    public RandomShiftPredicate(float probability) {
        this.probability = probability;
    }

    @Override
    public boolean needsToShift(MiruTenantId tenantId,
        MiruPartitionId partitionId,
        Collection<HostHeartbeat> hostHeartbeats,
        List<MiruPartition> partitions) {
        return random.nextFloat() < probability;
    }
}
