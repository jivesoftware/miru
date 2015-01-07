package com.jivesoftware.os.miru.api;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 *
 */
public class MiruTopologyStatus {

    public final MiruPartition partition;

    @JsonCreator
    public MiruTopologyStatus(@JsonProperty("partition") MiruPartition partition) {
        this.partition = partition;
    }

    @Override
    public String toString() {
        return "MiruTopologyStatus{" +
            "partition=" + partition +
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

        if (partition != null ? !partition.equals(that.partition) : that.partition != null) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        int result = partition != null ? partition.hashCode() : 0;
        return result;
    }
}
