package com.jivesoftware.os.miru.api;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 *
 */
public class MiruTopologyStatus {

    public final MiruPartition partition;
    public final MiruPartitionCoordMetrics metrics;

    @JsonCreator
    public MiruTopologyStatus(@JsonProperty("partition") MiruPartition partition, @JsonProperty("metrics") MiruPartitionCoordMetrics metrics) {
        this.partition = partition;
        this.metrics = metrics;
    }

    @Override
    public String toString() {
        return "MiruTopologyStatus{" +
            "partition=" + partition +
            ", metrics=" + metrics +
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

        MiruTopologyStatus that = (MiruTopologyStatus) o;

        if (metrics != null ? !metrics.equals(that.metrics) : that.metrics != null) {
            return false;
        }
        if (partition != null ? !partition.equals(that.partition) : that.partition != null) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        int result = partition != null ? partition.hashCode() : 0;
        result = 31 * result + (metrics != null ? metrics.hashCode() : 0);
        return result;
    }
}
