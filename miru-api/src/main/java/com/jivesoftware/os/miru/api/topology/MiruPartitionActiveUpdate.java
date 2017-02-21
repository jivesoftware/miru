package com.jivesoftware.os.miru.api.topology;

import com.jivesoftware.os.miru.api.base.MiruTenantId;

/**
 *
 */
public class MiruPartitionActiveUpdate {

    public MiruTenantId tenantId;
    public int partitionId;
    public boolean hosted;
    public MiruPartitionActive partitionActive;

    public MiruPartitionActiveUpdate() {
    }

    public MiruPartitionActiveUpdate(MiruTenantId tenantId,
        int partitionId,
        boolean hosted,
        MiruPartitionActive partitionActive) {
        this.tenantId = tenantId;
        this.partitionId = partitionId;
        this.hosted = hosted;
        this.partitionActive = partitionActive;
    }
}
