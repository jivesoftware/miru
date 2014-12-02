package com.jivesoftware.os.miru.api.activity.schema;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
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
    private final List<MiruFieldDefinition>[] fieldToPairedLatestFieldDefinitions;
    private final List<MiruFieldDefinition>[] fieldToBloomFieldDefinitions;

    private ImmutableList<Integer> fieldIds; // lazy initialized
    private ImmutableList<MiruFieldDefinition> fieldsWithLatest; // lazy initialized
    private ImmutableList<MiruFieldDefinition> fieldsWithPairedLatest; // lazy initialized
    private ImmutableList<MiruFieldDefinition> fieldsWithBloom; // lazy initialized

    public MiruSchema(MiruFieldDefinition... fieldDefinitions) {
        this(fieldDefinitions, new MiruPropertyDefinition[0]);
    }

    @JsonCreator
    public MiruSchema(@JsonProperty("fieldDefinitions") MiruFieldDefinition[] fieldDefinitions,
        @JsonProperty("propertyDefinitions") MiruPropertyDefinition[] propertyDefinitions) {

        this.fieldDefinitions = fieldDefinitions;
        this.fieldNameToId = Maps.newHashMap();
        for (MiruFieldDefinition fieldDefinition : fieldDefinitions) {
            fieldNameToId.put(fieldDefinition.name, fieldDefinition.fieldId);
        }

        this.propertyDefinitions = propertyDefinitions;
        this.propNameToId = Maps.newHashMap();
        for (MiruPropertyDefinition propertyDefinition : propertyDefinitions) {
            propNameToId.put(propertyDefinition.name, propertyDefinition.propId);
        }

        this.fieldToPairedLatestFieldDefinitions = new List[fieldDefinitions.length];
        this.fieldToBloomFieldDefinitions = new List[fieldDefinitions.length];
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
    public ImmutableList<MiruFieldDefinition> getFieldsWithLatest() {
        if (fieldsWithLatest == null) {
            ImmutableList.Builder<MiruFieldDefinition> builder = ImmutableList.builder();
            for (MiruFieldDefinition fieldDefinition : fieldDefinitions) {
                if (fieldDefinition.indexLatest) {
                    builder.add(fieldDefinition);
                }
            }
            fieldsWithLatest = builder.build();
        }
        return fieldsWithLatest;
    }

    @JsonIgnore
    public ImmutableList<MiruFieldDefinition> getFieldsWithPairedLatest() {
        if (fieldsWithPairedLatest == null) {
            ImmutableList.Builder<MiruFieldDefinition> builder = ImmutableList.builder();
            for (MiruFieldDefinition fieldDefinition : fieldDefinitions) {
                if (!fieldDefinition.pairedLatestFieldNames.isEmpty()) {
                    builder.add(fieldDefinition);
                }
            }
            fieldsWithPairedLatest = builder.build();
        }
        return fieldsWithPairedLatest;
    }

    @JsonIgnore
    public ImmutableList<MiruFieldDefinition> getFieldsWithBloom() {
        if (fieldsWithBloom == null) {
            ImmutableList.Builder<MiruFieldDefinition> builder = ImmutableList.builder();
            for (MiruFieldDefinition fieldDefinition : fieldDefinitions) {
                if (!fieldDefinition.bloomFieldNames.isEmpty()) {
                    builder.add(fieldDefinition);
                }
            }
            fieldsWithBloom = builder.build();
        }
        return fieldsWithBloom;
    }

    @JsonIgnore
    public List<MiruFieldDefinition> getPairedLatestFieldDefinitions(int fieldId) {
        List<MiruFieldDefinition> pairedLatestFieldDefinitions = fieldToPairedLatestFieldDefinitions[fieldId];
        if (pairedLatestFieldDefinitions == null) {
            pairedLatestFieldDefinitions = Lists.newArrayList();
            List<String> pairedLatestFieldNames = fieldDefinitions[fieldId].pairedLatestFieldNames;
            Lists.newArrayListWithCapacity(pairedLatestFieldNames.size());
            for (String pairedLatestFieldName : pairedLatestFieldNames) {
                int pairedLatestFieldId = getFieldId(pairedLatestFieldName);
                if (pairedLatestFieldId >= 0) {
                    pairedLatestFieldDefinitions.add(fieldDefinitions[pairedLatestFieldId]);
                }
            }
            fieldToPairedLatestFieldDefinitions[fieldId] = pairedLatestFieldDefinitions;
        }
        return pairedLatestFieldDefinitions;
    }

    @JsonIgnore
    public List<MiruFieldDefinition> getBloomFieldDefinitions(int fieldId) {
        List<MiruFieldDefinition> bloomFieldDefinitions = fieldToBloomFieldDefinitions[fieldId];
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
            fieldToBloomFieldDefinitions[fieldId] = bloomFieldDefinitions;
        }
        return bloomFieldDefinitions;
    }
}
