package com.jivesoftware.os.miru.api.activity;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 *
 */
public class MiruPartitionId implements Comparable<MiruPartitionId> {

    private final int id;

    private MiruPartitionId(int id) {
        this.id = id;
    }

    @JsonCreator
    public static MiruPartitionId of(@JsonProperty("id") int id) {
        return new MiruPartitionId(id);
    }

    public int getId() {
        return id;
    }

    @Override
    public int compareTo(MiruPartitionId o) {
        return Integer.compare(id, o.id);
    }

    public MiruPartitionId next() {
        return new MiruPartitionId(id + 1);
    }

    public MiruPartitionId prev() {
        return id > 0 ? new MiruPartitionId(id - 1) : null;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        MiruPartitionId that = (MiruPartitionId) o;

        return id == that.id;
    }

    @Override
    public int hashCode() {
        return id;
    }

    @Override
    public String toString() {
        return String.valueOf(id);
    }
}
