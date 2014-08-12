package com.jivesoftware.os.miru.api;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 *
 */
public class MiruPartitionCoordMetrics {

    public final long sizeInMemory;
    public final long sizeOnDisk;

    @JsonCreator
    public MiruPartitionCoordMetrics(
        @JsonProperty("sizeInMemory") long sizeInMemory,
        @JsonProperty("sizeOnDisk") long sizeOnDisk) {
        this.sizeInMemory = sizeInMemory;
        this.sizeOnDisk = sizeOnDisk;
    }

    @Override
    public String toString() {
        return "MiruPartitionCoordMetrics{" +
            "sizeInMemory=" + sizeInMemory +
            ", sizeOnDisk=" + sizeOnDisk +
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

        MiruPartitionCoordMetrics that = (MiruPartitionCoordMetrics) o;

        if (sizeInMemory != that.sizeInMemory) {
            return false;
        }
        if (sizeOnDisk != that.sizeOnDisk) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        int result = (int) (sizeInMemory ^ (sizeInMemory >>> 32));
        result = 31 * result + (int) (sizeOnDisk ^ (sizeOnDisk >>> 32));
        return result;
    }
}
