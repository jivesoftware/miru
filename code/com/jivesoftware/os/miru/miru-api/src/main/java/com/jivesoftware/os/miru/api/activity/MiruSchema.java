package com.jivesoftware.os.miru.api.activity;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;

import java.util.Map;

/**
 *
 * @author jonathan
 */
public class MiruSchema {

    public static final String RESERVED_AGGREGATE = "~";

    private final Map<String, Integer> fieldNameToId;
    private final MiruFieldDefinition[] fieldDefinitions;

    public MiruSchema(MiruFieldDefinition... fieldDefinitions) {
        this.fieldDefinitions = fieldDefinitions;
        this.fieldNameToId = Maps.newHashMap();
        for (MiruFieldDefinition fieldDefinition : fieldDefinitions) {
            Preconditions.checkArgument(!RESERVED_AGGREGATE.equals(fieldDefinition.name), "Cannot use reserved aggregate field name");
            fieldNameToId.put(fieldDefinition.name, fieldDefinition.fieldId);
        }
    }

    public int getFieldId(String fieldName) {
        Integer fieldId = fieldNameToId.get(fieldName);
        if (fieldId == null) {
            return -1;
        }
        return fieldId;
    }

    public MiruFieldDefinition getFieldDefinition(int fieldId) {
        return fieldDefinitions[fieldId];
    }

    public int fieldCount() {
        return fieldDefinitions.length;
    }
}
