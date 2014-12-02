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
    public final boolean indexLatest;
    public final List<String> pairedLatestFieldNames;
    public final List<String> bloomFieldNames;

    public MiruFieldDefinition(int fieldId, String name) {
        this(fieldId, name, false, Collections.<String>emptyList(), Collections.<String>emptyList());
    }

    @JsonCreator
    public MiruFieldDefinition(@JsonProperty("fieldId") int fieldId,
        @JsonProperty("name") String name,
        @JsonProperty("indexLatest") boolean indexLatest,
        @JsonProperty("pairedLatestFieldNames") List<String> pairedLatestFieldNames,
        @JsonProperty("bloomFieldNames") List<String> bloomFieldNames) {

        this.fieldId = fieldId;
        this.name = name;
        this.indexLatest = indexLatest;
        this.pairedLatestFieldNames = pairedLatestFieldNames;
        this.bloomFieldNames = bloomFieldNames;
    }
}
