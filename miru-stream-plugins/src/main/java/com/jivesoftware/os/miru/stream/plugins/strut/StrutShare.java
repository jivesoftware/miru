package com.jivesoftware.os.miru.stream.plugins.strut;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.jivesoftware.os.miru.api.activity.MiruPartitionId;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import java.io.Serializable;

/**
 *
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class StrutShare implements Serializable {

    public final MiruTenantId tenantId;
    public final MiruPartitionId partitionId;
    public final CatwalkDefinition catwalkDefinition;
    public final String modelId;
    public final int pivotFieldId;

    @JsonCreator
    public StrutShare(@JsonProperty("tenantId") MiruTenantId tenantId,
        @JsonProperty("partitionId") MiruPartitionId partitionId,
        @JsonProperty("catwalkDefinition") CatwalkDefinition catwalkDefinition,
        @JsonProperty("modelId") String modelId,
        @JsonProperty("pivotFieldId") int pivotFieldId) {
        this.tenantId = tenantId;
        this.partitionId = partitionId;
        this.modelId = modelId;
        this.pivotFieldId = pivotFieldId;
        this.catwalkDefinition = catwalkDefinition;
    }
}
