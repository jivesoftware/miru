package com.jivesoftware.os.miru.api.query.config;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.jivesoftware.os.miru.api.MiruPartition;
import java.util.Collections;
import java.util.List;

public class PartitionsForTenantResult {

    public static final PartitionsForTenantResult DEFAULT_RESULT = new PartitionsForTenantResult(Collections.<MiruPartition>emptyList());

    private final List<MiruPartition> partitions;

    @JsonCreator
    public PartitionsForTenantResult(@JsonProperty("partitions") List<MiruPartition> partitions) {
        this.partitions = partitions;
    }

    public List<MiruPartition> getPartitions() {
        return partitions;
    }
}
