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
}
