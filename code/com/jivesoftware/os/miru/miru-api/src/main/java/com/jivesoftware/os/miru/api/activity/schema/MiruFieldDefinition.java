package com.jivesoftware.os.miru.api.activity.schema;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Collections;
import java.util.List;

/**
 *
 */
public class MiruFieldDefinition {

    public final int fieldId;
    public final String name;
    public final boolean writeTimeAggregate;
    public final List<String> aggregateFieldNames;
    public final List<String> bloomFieldNames;

    public MiruFieldDefinition(int fieldId, String name) {
        this(fieldId, name, false, Collections.<String>emptyList(), Collections.<String>emptyList());
    }

    @JsonCreator
    public MiruFieldDefinition(@JsonProperty("fieldId") int fieldId,
        @JsonProperty("name") String name,
        @JsonProperty("writeTimeAggregate") boolean writeTimeAggregate,
        @JsonProperty("aggregateFieldNames") List<String> aggregateFieldNames,
        @JsonProperty("bloomFieldNames") List<String> bloomFieldNames) {

        this.fieldId = fieldId;
        this.name = name;
        this.writeTimeAggregate = writeTimeAggregate;
        this.aggregateFieldNames = aggregateFieldNames;
        this.bloomFieldNames = bloomFieldNames;
    }
}
