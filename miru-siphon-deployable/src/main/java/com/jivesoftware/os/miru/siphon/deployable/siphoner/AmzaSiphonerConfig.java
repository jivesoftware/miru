package com.jivesoftware.os.miru.siphon.deployable.siphoner;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.jivesoftware.os.amza.api.partition.PartitionName;
import com.jivesoftware.os.miru.api.base.MiruTenantId;

/**
 * Created by jonathan.colt on 4/27/17.
 */
public class AmzaSiphonerConfig {

    public final long uniqueId;
    public final String name;
    public final String description;
    public final String siphonPluginName;
    public final PartitionName partitionName;
    public final MiruTenantId destinationTenantId;
    public final int batchSize;

    @JsonCreator
    public AmzaSiphonerConfig(@JsonProperty("uniqueId") long uniqueId,
        @JsonProperty("name") String name,
        @JsonProperty("description") String description,
        @JsonProperty("siphonPluginName") String siphonPluginName,
        @JsonProperty("partitionName") PartitionName partitionName,
        @JsonProperty("destinationTenantId") MiruTenantId destinationTenantId,
        @JsonProperty("batchSize") int batchSize) {

        this.uniqueId = uniqueId;
        this.name = name;
        this.description = description;
        this.siphonPluginName = siphonPluginName;
        this.partitionName = partitionName;
        this.destinationTenantId = destinationTenantId;
        this.batchSize = batchSize;
    }
}
