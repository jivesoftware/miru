package com.jivesoftware.os.miru.api.topology;

import com.jivesoftware.os.miru.api.activity.MiruPartitionId;
import com.jivesoftware.os.miru.api.base.MiruTenantId;

/**
 * @author jonathan.colt
 */
public class MiruIngressUpdate {

    public MiruTenantId tenantId;
    public MiruPartitionId partitionId;
    public RangeMinMax minMax;
    public long ingressTimestamp; // -1 means unchanged
    public boolean absolute;

    public MiruIngressUpdate() {
    }

    public MiruIngressUpdate(MiruTenantId tenantId, MiruPartitionId partitionId, RangeMinMax minMax, long ingressTimestamp, boolean absolute) {
        this.tenantId = tenantId;
        this.partitionId = partitionId;
        this.minMax = minMax;
        this.ingressTimestamp = ingressTimestamp;
        this.absolute = absolute;
    }

    @Override
    public String toString() {
        return "MiruIngressUpdate{" +
            "tenantId=" + tenantId +
            ", partitionId=" + partitionId +
            ", minMax=" + minMax +
            ", ingressTimestamp=" + ingressTimestamp +
            ", absolute=" + absolute +
            '}';
    }

}
