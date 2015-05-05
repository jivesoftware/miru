package com.jivesoftware.os.miru.tools.deployable.region.input;

import com.google.common.base.Optional;
import com.jivesoftware.os.miru.api.base.MiruTenantId;

/**
 *
 */
public class MiruLookupRegionInput {

    private final Optional<MiruTenantId> tenantId;
    private final Optional<Long> afterTimestamp;
    private final Optional<Integer> limit;

    public MiruLookupRegionInput(Optional<MiruTenantId> tenantId, Optional<Long> afterTimestamp, Optional<Integer> limit) {
        this.tenantId = tenantId;
        this.afterTimestamp = afterTimestamp;
        this.limit = limit;
    }

    public Optional<MiruTenantId> getTenantId() {
        return tenantId;
    }

    public Optional<Long> getAfterTimestamp() {
        return afterTimestamp;
    }

    public Optional<Integer> getLimit() {
        return limit;
    }
}
