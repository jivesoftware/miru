package com.jivesoftware.os.miru.catwalk.deployable;

import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.catwalk.shared.CatwalkQuery;

/**
 *
 */
public class UpdateModelRequest {

    public final MiruTenantId tenantId;
    public final String catwalkId;
    public final String modelId;
    public final int partitionId;
    public final CatwalkQuery catwalkQuery;
    public final long timestamp;

    public boolean markProcessed;
    public boolean removeFromQueue;
    public boolean delayInQueue;

    public UpdateModelRequest(MiruTenantId tenantId,
        String catwalkId,
        String modelId,
        int partitionId,
        CatwalkQuery catwalkQuery,
        long timestamp) {
        this.tenantId = tenantId;
        this.catwalkId = catwalkId;
        this.modelId = modelId;
        this.partitionId = partitionId;
        this.catwalkQuery = catwalkQuery;
        this.timestamp = timestamp;
    }

    @Override
    public String toString() {
        return "UpdateModelRequest{" +
            "tenantId=" + tenantId +
            ", catwalkId='" + catwalkId + '\'' +
            ", modelId='" + modelId + '\'' +
            ", partitionId=" + partitionId +
            ", catwalkQuery=" + catwalkQuery +
            ", timestamp=" + timestamp +
            ", markProcessed=" + markProcessed +
            ", removeFromQueue=" + removeFromQueue +
            ", delayInQueue=" + delayInQueue +
            '}';
    }
}
