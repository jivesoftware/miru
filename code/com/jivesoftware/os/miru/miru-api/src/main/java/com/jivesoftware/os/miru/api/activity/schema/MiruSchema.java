package com.jivesoftware.os.miru.api.activity.schema;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * @author jonathan
 */
public class MiruSchema {

    public static final String RESERVED_AGGREGATE = "~";

    // Serializable fields
    private final String name;
    private final long version;
    private final MiruFieldDefinition[] fieldDefinitions;
    private final MiruPropertyDefinition[] propertyDefinitions;
    private final Map<String, List<String>> pairedLatest;
    private final Map<String, List<String>> bloom;

    // Lookup fields
    private final Map<String, Integer> fieldNameToId;
    private final Map<String, Integer> propNameToId;
    private final ImmutableList<MiruFieldDefinition>[] fieldToPairedLatestFieldDefinitions;
    private final ImmutableList<MiruFieldDefinition>[] fieldToBloomFieldDefinitions;

    // Traversal fields
    private final ImmutableList<Integer> fieldIds;
    private final ImmutableList<MiruFieldDefinition> fieldsWithLatest;
    private final ImmutableList<MiruFieldDefinition> fieldsWithPairedLatest;
    private final ImmutableList<MiruFieldDefinition> fieldsWithBloom;

    MiruSchema(String name,
        long version,
        MiruFieldDefinition[] fieldDefinitions,
        MiruPropertyDefinition[] propertyDefinitions,
        Map<String, List<String>> pairedLatest,
        Map<String, List<String>> bloom,
        ImmutableMap<String, Integer> fieldNameToId,
        ImmutableMap<String, Integer> propNameToId,
        ImmutableList<MiruFieldDefinition>[] fieldToPairedLatestFieldDefinitions,
        ImmutableList<MiruFieldDefinition>[] fieldToBloomFieldDefinitions,
        ImmutableList<Integer> fieldIds,
        ImmutableList<MiruFieldDefinition> fieldsWithLatest,
        ImmutableList<MiruFieldDefinition> fieldsWithPairedLatest,
        ImmutableList<MiruFieldDefinition> fieldsWithBloom) {
        this.name = name;
        this.version = version;
        this.fieldDefinitions = fieldDefinitions;
        this.propertyDefinitions = propertyDefinitions;
        this.pairedLatest = pairedLatest;
        this.bloom = bloom;
        this.fieldNameToId = fieldNameToId;
        this.propNameToId = propNameToId;
        this.fieldToPairedLatestFieldDefinitions = fieldToPairedLatestFieldDefinitions;
        this.fieldToBloomFieldDefinitions = fieldToBloomFieldDefinitions;
        this.fieldIds = fieldIds;
        this.fieldsWithLatest = fieldsWithLatest;
        this.fieldsWithPairedLatest = fieldsWithPairedLatest;
        this.fieldsWithBloom = fieldsWithBloom;
    }

    @JsonCreator
    public static MiruSchema fromJson(@JsonProperty("name") String name,
        @JsonProperty("version") long version,
        @JsonProperty("fieldDefinitions") MiruFieldDefinition[] fieldDefinitions,
        @JsonProperty("propertyDefinitions") MiruPropertyDefinition[] propertyDefinitions,
        @JsonProperty("pairedLatest") Map<String, List<String>> pairedLatest,
        @JsonProperty("bloom") Map<String, List<String>> bloom) {

        return new Builder(name, version)
            .setFieldDefinitions(fieldDefinitions)
            .setPropertyDefinitions(propertyDefinitions)
            .setPairedLatest(pairedLatest)
            .setBloom(bloom)
            .build();
    }

    public String getName() {
        return name;
    }

    public long getVersion() {
        return version;
    }

    public MiruFieldDefinition[] getFieldDefinitions() {
        return fieldDefinitions;
    }

    public MiruPropertyDefinition[] getPropertyDefinitions() {
        return propertyDefinitions;
    }

    public Map<String, List<String>> getPairedLatest() {
        return pairedLatest;
    }

    public Map<String, List<String>> getBloom() {
        return bloom;
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
        return fieldIds;
    }

    @JsonIgnore
    public ImmutableList<MiruFieldDefinition> getFieldsWithLatest() {
        return fieldsWithLatest;
    }

    @JsonIgnore
    public ImmutableList<MiruFieldDefinition> getFieldsWithPairedLatest() {
        return fieldsWithPairedLatest;
    }

    @JsonIgnore
    public ImmutableList<MiruFieldDefinition> getFieldsWithBloom() {
        return fieldsWithBloom;
    }

    @JsonIgnore
    public List<MiruFieldDefinition> getPairedLatestFieldDefinitions(int fieldId) {
        return fieldToPairedLatestFieldDefinitions[fieldId];
    }

    @JsonIgnore
    public List<MiruFieldDefinition> getBloomFieldDefinitions(int fieldId) {
        return fieldToBloomFieldDefinitions[fieldId];
    }

    public static class Builder {

        private static final MiruFieldDefinition[] NO_FIELDS = new MiruFieldDefinition[0];
        private static final MiruPropertyDefinition[] NO_PROPERTIES = new MiruPropertyDefinition[0];
        private static final Map<String, List<String>> NO_PAIRED_LATEST = Collections.emptyMap();
        private static final Map<String, List<String>> NO_BLOOM = Collections.emptyMap();

        private final String name;
        private final long version;

        private MiruFieldDefinition[] fieldDefinitions = NO_FIELDS;
        private MiruPropertyDefinition[] propertyDefinitions = NO_PROPERTIES;
        private Map<String, List<String>> pairedLatest = NO_PAIRED_LATEST;
        private Map<String, List<String>> bloom = NO_BLOOM;

        public Builder(String name, long version) {
            this.name = name;
            this.version = version;
        }

        public Builder setFieldDefinitions(MiruFieldDefinition[] fieldDefinitions) {
            this.fieldDefinitions = fieldDefinitions;
            return this;
        }

        public Builder setPropertyDefinitions(MiruPropertyDefinition[] propertyDefinitions) {
            this.propertyDefinitions = propertyDefinitions;
            return this;
        }

        public Builder setPairedLatest(Map<String, List<String>> pairedLatest) {
            this.pairedLatest = pairedLatest;
            return this;
        }

        public Builder setBloom(Map<String, List<String>> bloom) {
            this.bloom = bloom;
            return this;
        }

        public MiruSchema build() {
            int largestFieldId = -1;
            for (MiruFieldDefinition fieldDefinition : fieldDefinitions) {
                largestFieldId = Math.max(largestFieldId, fieldDefinition.fieldId);
            }

            Map<String, Integer> fieldNameToId = Maps.newHashMap();
            Map<String, Integer> propNameToId = Maps.newHashMap();
            ImmutableList<MiruFieldDefinition>[] fieldToPairedLatestFieldDefinitions = new ImmutableList[fieldDefinitions.length];
            ImmutableList<MiruFieldDefinition>[] fieldToBloomFieldDefinitions = new ImmutableList[fieldDefinitions.length];

            for (MiruFieldDefinition fieldDefinition : fieldDefinitions) {
                fieldNameToId.put(fieldDefinition.name, fieldDefinition.fieldId);
            }

            for (MiruPropertyDefinition propertyDefinition : propertyDefinitions) {
                propNameToId.put(propertyDefinition.name, propertyDefinition.propId);
            }

            ImmutableList.Builder<Integer> fieldIdsBuilder = new ImmutableList.Builder<>();
            ImmutableList.Builder<MiruFieldDefinition> fieldsWithLatestBuilder = new ImmutableList.Builder<>();
            ImmutableList.Builder<MiruFieldDefinition> fieldsWithPairedLatestBuilder = new ImmutableList.Builder<>();
            ImmutableList.Builder<MiruFieldDefinition> fieldsWithBloomBuilder = new ImmutableList.Builder<>();

            for (MiruFieldDefinition fieldDefinition : fieldDefinitions) {
                fieldIdsBuilder.add(fieldDefinition.fieldId);

                if (fieldDefinition.type == MiruFieldDefinition.Type.singleTermIndexLatest) {
                    fieldsWithLatestBuilder.add(fieldDefinition);
                }

                ImmutableList.Builder<MiruFieldDefinition> pairedLatestFieldDefinitionsBuilder = new ImmutableList.Builder<>();
                List<String> pairedLatestFieldNames = pairedLatest.get(fieldDefinition.name);
                if (pairedLatestFieldNames != null) {
                    fieldsWithPairedLatestBuilder.add(fieldDefinition);
                    for (String pairedLatestFieldName : pairedLatestFieldNames) {
                        MiruFieldDefinition pairedLatestFieldDefinition = fieldDefinitions[fieldNameToId.get(pairedLatestFieldName)];
                        if (pairedLatestFieldDefinition.type == MiruFieldDefinition.Type.multiTerm) {
                            throw new IllegalArgumentException("Paired latest cannot be applied to multi-term field: " +
                                fieldDefinition.name + " -> " + pairedLatestFieldName);
                        }
                        pairedLatestFieldDefinitionsBuilder.add(pairedLatestFieldDefinition);
                    }
                }
                fieldToPairedLatestFieldDefinitions[fieldDefinition.fieldId] = pairedLatestFieldDefinitionsBuilder.build();

                ImmutableList.Builder<MiruFieldDefinition> bloomFieldDefinitionsBuilder = new ImmutableList.Builder<>();
                List<String> bloomFieldNames = bloom.get(fieldDefinition.name);
                if (bloomFieldNames != null) {
                    fieldsWithBloomBuilder.add(fieldDefinition);
                    for (String bloomFieldName : bloomFieldNames) {
                        bloomFieldDefinitionsBuilder.add(fieldDefinitions[fieldNameToId.get(bloomFieldName)]);
                    }
                }
                fieldToBloomFieldDefinitions[fieldDefinition.fieldId] = bloomFieldDefinitionsBuilder.build();
            }

            ImmutableList<Integer> fieldIds = fieldIdsBuilder.build();
            ImmutableList<MiruFieldDefinition> fieldsWithLatest = fieldsWithLatestBuilder.build();
            ImmutableList<MiruFieldDefinition> fieldsWithPairedLatest = fieldsWithPairedLatestBuilder.build();
            ImmutableList<MiruFieldDefinition> fieldsWithBloom = fieldsWithBloomBuilder.build();

            return new MiruSchema(name, version, fieldDefinitions, propertyDefinitions, pairedLatest, bloom, ImmutableMap.copyOf(fieldNameToId),
                ImmutableMap.copyOf(propNameToId), fieldToPairedLatestFieldDefinitions, fieldToBloomFieldDefinitions,
                fieldIds, fieldsWithLatest, fieldsWithPairedLatest, fieldsWithBloom);
        }
    }
}
