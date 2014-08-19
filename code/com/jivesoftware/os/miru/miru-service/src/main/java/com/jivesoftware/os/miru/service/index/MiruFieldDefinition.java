package com.jivesoftware.os.miru.service.index;

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

    public MiruFieldDefinition(int fieldId, String name, boolean writeTimeAggregate, List<String> aggregateFieldNames, List<String> bloomFieldNames) {
        this.fieldId = fieldId;
        this.name = name;
        this.writeTimeAggregate = writeTimeAggregate;
        this.aggregateFieldNames = aggregateFieldNames;
        this.bloomFieldNames = bloomFieldNames;
    }
}
