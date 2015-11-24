package com.jivesoftware.os.miru.api.topology;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.jivesoftware.os.miru.api.activity.MiruPartitionId;
import com.jivesoftware.os.miru.api.base.MiruTenantId;

/**
 *
 */
public class MiruPartitionStatus {

    private final MiruTenantId tenantId;
    private final MiruPartitionId partitionId;
    private final long lastIngressTimestamp;
    private final long destroyAfterTimestamp;

    @JsonCreator
    public MiruPartitionStatus(@JsonProperty("tenantId") MiruTenantId tenantId,
        @JsonProperty("partitionId") MiruPartitionId partitionId,
        @JsonProperty("lastIngressTimestamp") long lastIngressTimestamp,
        @JsonProperty("destroyAfterTimestamp") long destroyAfterTimestamp) {
        this.tenantId = tenantId;
        this.partitionId = partitionId;
        this.lastIngressTimestamp = lastIngressTimestamp;
        this.destroyAfterTimestamp = destroyAfterTimestamp;
    }

    public MiruTenantId getTenantId() {
        return tenantId;
    }

    public MiruPartitionId getPartitionId() {
        return partitionId;
    }

    public long getLastIngressTimestamp() {
        return lastIngressTimestamp;
    }

    public long getDestroyAfterTimestamp() {
        return destroyAfterTimestamp;
    }

    @Override
    public String toString() {
        return "MiruPartitionStatus{" +
            "tenantId=" + tenantId +
            ", partitionId=" + partitionId +
            ", lastIngressTimestamp=" + lastIngressTimestamp +
            ", destroyAfterTimestamp=" + destroyAfterTimestamp +
            '}';
    }
}
