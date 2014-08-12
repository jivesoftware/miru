package com.jivesoftware.os.miru.api;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

/** @author jonathan */
public class MiruPartitionCoordInfo {

    public final MiruPartitionState state;
    public final MiruBackingStorage storage;

    @JsonCreator
    public MiruPartitionCoordInfo(@JsonProperty("state") MiruPartitionState state, @JsonProperty("storage") MiruBackingStorage storage) {
        this.state = state;
        this.storage = storage;
    }

    public MiruPartitionCoordInfo copyToState(MiruPartitionState state) {
        return new MiruPartitionCoordInfo(state, storage);
    }

    public MiruPartitionCoordInfo copyToStorage(MiruBackingStorage storage) {
        return new MiruPartitionCoordInfo(state, storage);
    }

    @Override
    public String toString() {
        return "MiruPartitionCoordInfo{" +
            "state=" + state +
            ", storage=" + storage +
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

        MiruPartitionCoordInfo that = (MiruPartitionCoordInfo) o;

        if (state != that.state) {
            return false;
        }
        if (storage != that.storage) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        int result = state != null ? state.hashCode() : 0;
        result = 31 * result + (storage != null ? storage.hashCode() : 0);
        return result;
    }
}
