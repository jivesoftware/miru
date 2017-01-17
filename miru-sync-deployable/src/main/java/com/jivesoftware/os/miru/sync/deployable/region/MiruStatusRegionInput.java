package com.jivesoftware.os.miru.sync.deployable.region;

import com.jivesoftware.os.miru.api.base.MiruTenantId;

/**
 *
 */
public class MiruStatusRegionInput {

    public final String syncspaceName;
    public final MiruTenantId tenantId;

    public MiruStatusRegionInput(String syncspaceName, MiruTenantId tenantId) {
        this.syncspaceName = syncspaceName;
        this.tenantId = tenantId;
    }
}
