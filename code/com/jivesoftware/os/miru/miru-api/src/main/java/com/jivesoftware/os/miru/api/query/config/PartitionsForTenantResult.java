package com.jivesoftware.os.miru.api.query.config;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.Maps;
import com.jivesoftware.os.miru.api.MiruPartition;
import com.jivesoftware.os.miru.api.MiruPartitionState;
import java.util.Collection;
import java.util.Map;

public class PartitionsForTenantResult {

    public static final PartitionsForTenantResult DEFAULT_RESULT =
        new PartitionsForTenantResult(Maps.<MiruPartitionState, Collection<MiruPartition>>newHashMap());

    private final Map<MiruPartitionState, Collection<MiruPartition>> partitions;

    @JsonCreator
    public PartitionsForTenantResult(@JsonProperty("partitions") Map<MiruPartitionState, Collection<MiruPartition>> partitions) {
        this.partitions = partitions;
    }

    public Map<MiruPartitionState, Collection<MiruPartition>> getPartitions() {
        return partitions;
    }
}
