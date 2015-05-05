package com.jivesoftware.os.miru.api.topology;

import com.jivesoftware.os.miru.api.base.MiruTenantId;

/**
 *
 */
public class MiruPartitionActiveUpdate {

    public MiruTenantId tenantId;
    public int partitionId;
    public boolean hosted;
    public long activeUntilTimestamp;
    public long idleAfterTimestamp;
    public long destroyAfterTimestamp;

    public MiruPartitionActiveUpdate() {
    }

    public MiruPartitionActiveUpdate(MiruTenantId tenantId,
        int partitionId,
        boolean hosted,
        long activeUntilTimestamp,
        long idleAfterTimestamp,
        long destroyAfterTimestamp) {
        this.tenantId = tenantId;
        this.partitionId = partitionId;
        this.hosted = hosted;
        this.activeUntilTimestamp = activeUntilTimestamp;
        this.idleAfterTimestamp = idleAfterTimestamp;
        this.destroyAfterTimestamp = destroyAfterTimestamp;
    }
}
