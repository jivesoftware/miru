package com.jivesoftware.os.miru.stream.plugins.strut;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.jivesoftware.os.miru.api.activity.MiruPartitionId;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.catwalk.shared.CatwalkQuery.CatwalkDefinition;
import java.io.Serializable;
import java.util.Map;

/**
 *
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class StrutShare implements Serializable {

    public final MiruTenantId tenantId;
    public final MiruPartitionId partitionId;
    public final CatwalkDefinition catwalkDefinition;
    public final Map<String, CatwalkScorable> scorables;

    @JsonCreator
    public StrutShare(@JsonProperty("tenantId") MiruTenantId tenantId,
        @JsonProperty("partitionId") MiruPartitionId partitionId,
        @JsonProperty("catwalkDefinition") CatwalkDefinition catwalkDefinition,
        @JsonProperty("scorables") Map<String, CatwalkScorable> scorables) {
        this.tenantId = tenantId;
        this.partitionId = partitionId;
        this.catwalkDefinition = catwalkDefinition;
        this.scorables = scorables;
    }

    @Override
    public String toString() {
        return "StrutShare{" +
            "tenantId=" + tenantId +
            ", partitionId=" + partitionId +
            ", catwalkDefinition=" + catwalkDefinition +
            ", scorables=" + scorables +
            '}';
    }
}
