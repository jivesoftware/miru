package com.jivesoftware.os.miru.api.activity.schema;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 *
 */
public class MiruPropertyDefinition {

    public final int propId;
    public final String name;

    @JsonCreator
    public MiruPropertyDefinition(
        @JsonProperty("propId") int propId,
        @JsonProperty("name") String name) {
        this.propId = propId;
        this.name = name;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        MiruPropertyDefinition that = (MiruPropertyDefinition) o;

        if (propId != that.propId) {
            return false;
        }
        if (name != null ? !name.equals(that.name) : that.name != null) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        throw new UnsupportedOperationException("NOPE");
    }
}
