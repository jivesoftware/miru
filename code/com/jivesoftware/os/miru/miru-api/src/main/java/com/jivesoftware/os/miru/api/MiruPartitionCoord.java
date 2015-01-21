package com.jivesoftware.os.miru.api;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.jivesoftware.os.miru.api.activity.MiruPartitionId;
import com.jivesoftware.os.miru.api.base.MiruTenantId;

/** @author jonathan */
public class MiruPartitionCoord {

    public final MiruTenantId tenantId;
    public final MiruPartitionId partitionId;
    public final MiruHost host;

    @JsonCreator
    public MiruPartitionCoord(@JsonProperty("tenantId") MiruTenantId tenantId,
        @JsonProperty("partitionId") MiruPartitionId partitionId,
        @JsonProperty("host") MiruHost host) {
        this.tenantId = tenantId;
        this.partitionId = partitionId;
        this.host = host;
    }

    @Override
    public String toString() {
        return "MiruPartitionCoord{" +
            "tenantId=" + tenantId +
            ", partitionId=" + partitionId +
            ", host=" + host +
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

        MiruPartitionCoord that = (MiruPartitionCoord) o;

        if (host != null ? !host.equals(that.host) : that.host != null) {
            return false;
        }
        if (partitionId != null ? !partitionId.equals(that.partitionId) : that.partitionId != null) {
            return false;
        }
        return !(tenantId != null ? !tenantId.equals(that.tenantId) : that.tenantId != null);
    }

    @Override
    public int hashCode() {
        int result = tenantId != null ? tenantId.hashCode() : 0;
        result = 31 * result + (partitionId != null ? partitionId.hashCode() : 0);
        result = 31 * result + (host != null ? host.hashCode() : 0);
        return result;
    }
}
