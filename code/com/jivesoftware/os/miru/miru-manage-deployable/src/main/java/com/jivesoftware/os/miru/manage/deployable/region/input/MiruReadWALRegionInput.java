package com.jivesoftware.os.miru.manage.deployable.region.input;

import com.google.common.base.Optional;
import com.jivesoftware.os.miru.api.base.MiruTenantId;

/**
 *
 */
public class MiruReadWALRegionInput {

    private final Optional<MiruTenantId> tenantId;
    private final Optional<String> streamId;
    private final Optional<Boolean> sip;
    private final Optional<Long> afterTimestamp;
    private final Optional<Integer> limit;

    public MiruReadWALRegionInput(Optional<MiruTenantId> tenantId, Optional<String> streamId, Optional<Boolean> sip, Optional<Long> afterTimestamp,
        Optional<Integer> limit) {
        this.tenantId = tenantId;
        this.streamId = streamId;
        this.sip = sip;
        this.afterTimestamp = afterTimestamp;
        this.limit = limit;
    }

    public Optional<MiruTenantId> getTenantId() {
        return tenantId;
    }

    public Optional<String> getStreamId() {
        return streamId;
    }

    public Optional<Boolean> getSip() {
        return sip;
    }

    public Optional<Long> getAfterTimestamp() {
        return afterTimestamp;
    }

    public Optional<Integer> getLimit() {
        return limit;
    }
}
