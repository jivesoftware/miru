package com.jivesoftware.os.miru.api;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 *
 */
public class MiruPartition {

    public final MiruPartitionCoord coord;
    public final MiruPartitionCoordInfo info;

    @JsonCreator
    public MiruPartition(@JsonProperty("coord") MiruPartitionCoord coord, @JsonProperty("info") MiruPartitionCoordInfo info) {
        this.coord = coord;
        this.info = info;
    }

    @Override
    public String toString() {
        return "MiruPartition{" +
            "coord=" + coord +
            ", info=" + info +
            '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        MiruPartition that = (MiruPartition) o;

        if (coord != null ? !coord.equals(that.coord) : that.coord != null) {
            return false;
        }
        return !(info != null ? !info.equals(that.info) : that.info != null);
    }

    @Override
    public int hashCode() {
        int result = coord != null ? coord.hashCode() : 0;
        result = 31 * result + (info != null ? info.hashCode() : 0);
        return result;
    }
}
