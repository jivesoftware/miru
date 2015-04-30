package com.jivesoftware.os.miru.api.topology;

import com.jivesoftware.os.miru.api.base.MiruTenantId;

/**
 *
 */
public class MiruPartitionActiveUpdate {

    public MiruTenantId tenantId;
    public int partitionId;
    public boolean hosted;
    public boolean active;
    public boolean idle;

    public MiruPartitionActiveUpdate() {
    }

    public MiruPartitionActiveUpdate(MiruTenantId tenantId,
        int partitionId,
        boolean hosted,
        boolean active,
        boolean idle) {
        this.tenantId = tenantId;
        this.partitionId = partitionId;
        this.hosted = hosted;
        this.active = active;
        this.idle = idle;
    }
}
