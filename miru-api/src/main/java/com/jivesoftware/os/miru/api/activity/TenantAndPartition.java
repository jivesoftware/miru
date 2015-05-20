package com.jivesoftware.os.miru.api.activity;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.jivesoftware.os.miru.api.base.MiruTenantId;

/**
 *
 */
public class TenantAndPartition {

    public final MiruTenantId tenantId;
    public final MiruPartitionId partitionId;

    @JsonCreator
    public TenantAndPartition(@JsonProperty("tenantId") MiruTenantId tenantId,
        @JsonProperty("partitionId") MiruPartitionId partitionId) {
        this.tenantId = tenantId;
        this.partitionId = partitionId;
    }

    @Override
    public String toString() {
        return "TenantAndPartition{" +
            "tenantId=" + tenantId +
            ", partitionId=" + partitionId +
            '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        TenantAndPartition that = (TenantAndPartition) o;

        if (tenantId != null ? !tenantId.equals(that.tenantId) : that.tenantId != null) {
            return false;
        }
        return !(partitionId != null ? !partitionId.equals(that.partitionId) : that.partitionId != null);

    }

    @Override
    public int hashCode() {
        int result = tenantId != null ? tenantId.hashCode() : 0;
        result = 31 * result + (partitionId != null ? partitionId.hashCode() : 0);
        return result;
    }
}
