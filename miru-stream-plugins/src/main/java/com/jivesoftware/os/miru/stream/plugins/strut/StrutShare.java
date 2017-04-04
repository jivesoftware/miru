package com.jivesoftware.os.miru.stream.plugins.strut;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.jivesoftware.os.miru.api.activity.MiruPartitionId;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.catwalk.shared.CatwalkQuery;
import com.jivesoftware.os.miru.catwalk.shared.Scored;
import java.io.Serializable;
import java.util.List;

/**
 *
 */
public class StrutShare implements Serializable {

    public final MiruTenantId tenantId;
    public final MiruPartitionId partitionId;
    public final CatwalkQuery catwalkQuery;
    public final String modelId;
    public final int pivotFieldId;
    public final List<Scored> updates;

    @JsonCreator
    public StrutShare(@JsonProperty("tenantId") MiruTenantId tenantId,
        @JsonProperty("partitionId") MiruPartitionId partitionId,
        @JsonProperty("catwalkQuery") CatwalkQuery catwalkQuery,
        @JsonProperty("modelId") String modelId,
        @JsonProperty("pivotFieldId") int pivotFieldId,
        @JsonProperty("updates") List<Scored> updates) {
        this.tenantId = tenantId;
        this.partitionId = partitionId;
        this.catwalkQuery = catwalkQuery;
        this.modelId = modelId;
        this.pivotFieldId = pivotFieldId;
        this.updates = updates;
    }
}
