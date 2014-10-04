package com.jivesoftware.os.miru.api.activity.schema;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import java.util.List;
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
    private final List<MiruFieldDefinition>[] fieldAggregateFieldDefinitions;
    private final List<MiruFieldDefinition>[] fieldBloominFieldDefinitions;

    private ImmutableList<Integer> fieldIds; // lazy initialized

    public MiruSchema(MiruFieldDefinition... fieldDefinitions) {
        this(fieldDefinitions, new MiruPropertyDefinition[0]);
    }

    @JsonCreator
    public MiruSchema(@JsonProperty("fieldDefinitions") MiruFieldDefinition[] fieldDefinitions,
        @JsonProperty("propertyDefinitions") MiruPropertyDefinition[] propertyDefinitions) {

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

        this.fieldAggregateFieldDefinitions = new List[fieldDefinitions.length];
        this.fieldBloominFieldDefinitions = new List[fieldDefinitions.length];
    }

    public MiruFieldDefinition[] getFieldDefinitions() {
        return fieldDefinitions;
    }

    public MiruPropertyDefinition[] getPropertyDefinitions() {
        return propertyDefinitions;
    }

    @JsonIgnore
    public int getFieldId(String fieldName) {
        Integer fieldId = fieldNameToId.get(fieldName);
        if (fieldId == null) {
            return -1;
        }
        return fieldId;
    }

    @JsonIgnore
    public MiruFieldDefinition getFieldDefinition(int fieldId) {
        return fieldDefinitions[fieldId];
    }

    @JsonIgnore
    public int fieldCount() {
        return fieldDefinitions.length;
    }

    @JsonIgnore
    public int getPropertyId(String propName) {
        Integer propId = propNameToId.get(propName);
        if (propId == null) {
            return -1;
        }
        return propId;
    }

    @JsonIgnore
    public MiruPropertyDefinition getPropertyDefinition(int propId) {
        return propertyDefinitions[propId];
    }

    @JsonIgnore
    public int propertyCount() {
        return propertyDefinitions.length;
    }

    @JsonIgnore
    public ImmutableList<Integer> getFieldIds() {
        if (fieldIds == null) {
            ImmutableList.Builder<Integer> builder = ImmutableList.builder();
            for (MiruFieldDefinition fieldDefinition : fieldDefinitions) {
                builder.add(fieldDefinition.fieldId);
            }
            fieldIds = builder.build();
        }
        return fieldIds;
    }

    @JsonIgnore
    public List<MiruFieldDefinition> getAggregateFieldDefinitions(int fieldId) {
        List<MiruFieldDefinition> aggregateFieldDefinitions = fieldAggregateFieldDefinitions[fieldId];
        if (aggregateFieldDefinitions == null) {
            aggregateFieldDefinitions = Lists.newArrayList();
            List<String> aggregateFieldNames = fieldDefinitions[fieldId].aggregateFieldNames;
            Lists.newArrayListWithCapacity(aggregateFieldNames.size());
            for (String aggregateFieldName : aggregateFieldNames) {
                int aggregateFieldId = getFieldId(aggregateFieldName);
                if (aggregateFieldId >= 0) {
                    aggregateFieldDefinitions.add(fieldDefinitions[aggregateFieldId]);
                }
            }
            fieldAggregateFieldDefinitions[fieldId] = aggregateFieldDefinitions;
        }
        return aggregateFieldDefinitions;
    }

    @JsonIgnore
    public List<MiruFieldDefinition> getBloominFieldDefinitions(int fieldId) {
        List<MiruFieldDefinition> bloomFieldDefinitions = fieldBloominFieldDefinitions[fieldId];
        if (bloomFieldDefinitions == null) {
            bloomFieldDefinitions = Lists.newArrayList();
            List<String> bloomFieldNames = fieldDefinitions[fieldId].bloomFieldNames;
            Lists.newArrayListWithCapacity(bloomFieldNames.size());
            for (String bloomFieldName : bloomFieldNames) {
                int bloomFieldId = getFieldId(bloomFieldName);
                if (bloomFieldId >= 0) {
                    bloomFieldDefinitions.add(fieldDefinitions[bloomFieldId]);
                }
            }
            fieldBloominFieldDefinitions[fieldId] = bloomFieldDefinitions;
        }
        return bloomFieldDefinitions;
    }
}
