package com.jivesoftware.os.miru.query;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.jivesoftware.os.miru.api.MiruPartition;
import com.jivesoftware.os.miru.api.MiruPartitionCoord;
import java.util.List;

/**
 *
 */
public class MiruSolution {

    public final MiruPartitionCoord coord;
    public final List<MiruPartition> orderedPartitions;
    public final List<MiruPartitionCoord> triedPartitions;
    public final long usedResultElapsed;
    public final long totalElapsed;

    @JsonCreator
    public MiruSolution(@JsonProperty("coord") MiruPartitionCoord coord,
            @JsonProperty("orderedPartitions") List<MiruPartition> orderedPartitions,
            @JsonProperty("triedPartitions") List<MiruPartitionCoord> triedPartitions,
            @JsonProperty("usedResultElapsed") long usedResultElapsed,
            @JsonProperty("totalElapsed") long totalElapsed) {
        this.coord = coord;
        this.orderedPartitions = orderedPartitions;
        this.triedPartitions = triedPartitions;
        this.usedResultElapsed = usedResultElapsed;
        this.totalElapsed = totalElapsed;
    }

    @Override
    public String toString() {
        return "MiruSolution{" +
                "coord=" + coord +
                ", orderedPartitions=" + orderedPartitions +
                ", triedPartitions=" + triedPartitions +
                ", usedResultElapsed=" + usedResultElapsed +
                ", totalElapsed=" + totalElapsed +
                '}';
    }
}
