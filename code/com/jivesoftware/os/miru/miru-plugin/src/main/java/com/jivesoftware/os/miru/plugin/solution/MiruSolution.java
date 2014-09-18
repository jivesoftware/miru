package com.jivesoftware.os.miru.plugin.solution;

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
    public final List<String> log;

    @JsonCreator
    public MiruSolution(@JsonProperty("usedPartition") MiruPartitionCoord usedPartition,
            @JsonProperty("usedResultElapsed") long usedResultElapsed,
            @JsonProperty("totalElapsed") long totalElapsed,
            @JsonProperty("orderedPartitions") List<MiruPartition> orderedPartitions,
            @JsonProperty("triedPartitions") List<MiruPartitionCoord> triedPartitions,
            @JsonProperty("log") List<String> log) {
        this.usedPartition = usedPartition;
        this.orderedPartitions = orderedPartitions;
        this.triedPartitions = triedPartitions;
        this.usedResultElapsed = usedResultElapsed;
        this.totalElapsed = totalElapsed;
        this.log = log;
    }

    @Override
    public String toString() {
        return "MiruSolution{"
                + "usedPartition=" + usedPartition
                + ", usedResultElapsed=" + usedResultElapsed
                + ", totalElapsed=" + totalElapsed
                + ", orderedPartitions=" + orderedPartitions
                + ", triedPartitions=" + triedPartitions
                + ", log=" + log
                + '}';
    }
}
