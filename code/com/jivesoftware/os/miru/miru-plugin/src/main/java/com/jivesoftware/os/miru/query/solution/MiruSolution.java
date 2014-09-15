package com.jivesoftware.os.miru.query.solution;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.jivesoftware.os.miru.api.MiruPartition;
import com.jivesoftware.os.miru.api.MiruPartitionCoord;
import java.util.List;

/**
 *
 */
public class MiruSolution {

    public final MiruPartitionCoord usedPartition;
    public final long usedResultElapsed;
    public final long totalElapsed;
    public final List<MiruPartition> orderedPartitions;
    public final List<MiruPartitionCoord> triedPartitions;

    @JsonCreator
    public MiruSolution(@JsonProperty (value = "usedPartition") MiruPartitionCoord usedPartition,
        @JsonProperty (value = "usedResultElapsed") long usedResultElapsed,
        @JsonProperty (value = "totalElapsed") long totalElapsed,
        @JsonProperty (value = "orderedPartitions") List<MiruPartition> orderedPartitions,
        @JsonProperty (value = "triedPartitions") List<MiruPartitionCoord> triedPartitions) {

        this.usedPartition = usedPartition;
        this.orderedPartitions = orderedPartitions;
        this.triedPartitions = triedPartitions;
        this.usedResultElapsed = usedResultElapsed;
        this.totalElapsed = totalElapsed;
    }

    @Override
    public String toString() {
        return "MiruSolution{"
            + "usedPartition=" + usedPartition
            + ", usedResultElapsed=" + usedResultElapsed
            + ", totalElapsed=" + totalElapsed
            + ", orderedPartitions=" + orderedPartitions
            + ", triedPartitions=" + triedPartitions
            + '}';
    }
}
