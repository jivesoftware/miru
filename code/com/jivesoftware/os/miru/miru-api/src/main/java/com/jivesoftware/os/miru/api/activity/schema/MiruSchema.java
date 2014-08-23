package com.jivesoftware.os.miru.api.activity.schema;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;

import java.util.Map;

/**
 * @author jonathan
 */
public class MiruSchema {

    public static final String RESERVED_AGGREGATE = "~";

    private final Map<String, Integer> fieldNameToId;
    private final Map<String, Integer> propNameToId;
    private final MiruFieldDefinition[] fieldDefinitions;
    private final MiruPropertyDefinition[] propertyDefinitions;

    public MiruSchema(MiruFieldDefinition... fieldDefinitions) {
        this(fieldDefinitions, new MiruPropertyDefinition[0]);
    }

    public MiruSchema(MiruFieldDefinition[] fieldDefinitions, MiruPropertyDefinition[] propertyDefinitions) {
        this.fieldDefinitions = fieldDefinitions;
        this.fieldNameToId = Maps.newHashMap();
        for (MiruFieldDefinition fieldDefinition : fieldDefinitions) {
            Preconditions.checkArgument(!RESERVED_AGGREGATE.equals(fieldDefinition.name), "Cannot use reserved aggregate field name");
            fieldNameToId.put(fieldDefinition.name, fieldDefinition.fieldId);
        }

        this.propertyDefinitions = propertyDefinitions;
        this.propNameToId = Maps.newHashMap();
        for (MiruPropertyDefinition propertyDefinition : propertyDefinitions) {
            propNameToId.put(propertyDefinition.name, propertyDefinition.propId);
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

    public int getPropertyId(String propName) {
        Integer propId = propNameToId.get(propName);
        if (propId == null) {
            return -1;
        }
        return propId;
    }

    public MiruPropertyDefinition getPropertyDefinition(int propId) {
        return propertyDefinitions[propId];
    }

    public int propertyCount() {
        return propertyDefinitions.length;
    }
}
