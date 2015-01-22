package com.jivesoftware.os.miru.api.activity.schema;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 *
 */
public class MiruFieldDefinition {

    public final int fieldId;
    public final String name;
    public final Type type;

    @JsonCreator
    public MiruFieldDefinition(@JsonProperty("fieldId") int fieldId,
        @JsonProperty("name") String name,
        @JsonProperty("type") Type type) {

        this.fieldId = fieldId;
        this.name = name;
        this.type = type;
    }

    public enum Type {
        singleTerm,
        singleTermIndexLatest,
        multiTerm
    }
}
