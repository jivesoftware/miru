package com.jivesoftware.os.miru.wal.deployable.region.input;

import com.google.common.base.Optional;
import com.jivesoftware.os.miru.api.activity.MiruPartitionId;
import com.jivesoftware.os.miru.api.base.MiruTenantId;

/**
 *
 */
public class MiruActivityWALRegionInput {

    private final Optional<MiruTenantId> tenantId;
    private final Optional<String> walType;
    private final Optional<MiruPartitionId> partitionId;
    private final Optional<Boolean> sip;
    private final Optional<Long> afterTimestamp;
    private final Optional<Integer> limit;

    public MiruActivityWALRegionInput(
        Optional<MiruTenantId> tenantId,
        Optional<String> walType,
        Optional<MiruPartitionId> partitionId,
        Optional<Boolean> sip,
        Optional<Long> afterTimestamp,
        Optional<Integer> limit) {
        this.tenantId = tenantId;
        this.walType = walType;
        this.partitionId = partitionId;
        this.sip = sip;
        this.afterTimestamp = afterTimestamp;
        this.limit = limit;
    }

    public Optional<MiruTenantId> getTenantId() {
        return tenantId;
    }

    public Optional<String> getWALType() {
        return walType;
    }

    public Optional<MiruPartitionId> getPartitionId() {
        return partitionId;
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
