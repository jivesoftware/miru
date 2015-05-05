package com.jivesoftware.os.miru.api.topology;

import com.jivesoftware.os.miru.api.base.MiruTenantId;

/**
 *
 * @author jonathan.colt
 */
public class MiruTenantTopologyUpdate {

    public MiruTenantId tenantId;
    public long timestamp;

    public MiruTenantTopologyUpdate() {
    }

    public MiruTenantTopologyUpdate(MiruTenantId tenantId, long timestamp) {
        this.tenantId = tenantId;
        this.timestamp = timestamp;
    }
}
