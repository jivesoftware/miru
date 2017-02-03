package com.jivesoftware.os.miru.api.activity.schema;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

/**
 * @author jonathan
 */
public class MiruSchema {

    public static final String RESERVED_AGGREGATE = "~";

    // Serializable fields
    private final String name;
    private final int version; // int for JSON compatibility
    private final MiruFieldDefinition[] fieldDefinitions;
    private final MiruPropertyDefinition[] propertyDefinitions;
    private final Map<String, List<String>> pairedLatest;
    private final Map<String, List<String>> bloom;
    private final Map<String, String[]> composite;
    private final Map<String, String[][]> compositePrefixWhitelist;

    // Lookup fields
    private final Map<String, Integer> fieldNameToId;
    private final Map<String, Integer> propNameToId;
    private final ImmutableList<MiruFieldDefinition>[] fieldToPairedLatestFieldDefinitions;
    private final ImmutableList<MiruFieldDefinition>[] fieldToBloomFieldDefinitions;

    // Composites
    private final CompositeFieldDefinition[][] fieldToCompositeDefinitions;

    // Traversal fields
    private final ImmutableList<Integer> fieldIds;
    private final ImmutableList<MiruFieldDefinition> fieldsWithFirst;
    private final ImmutableList<MiruFieldDefinition> fieldsWithLatest;
    private final ImmutableList<MiruFieldDefinition> fieldsWithPairedLatest;
    private final ImmutableList<MiruFieldDefinition> fieldsWithBloom;

    MiruSchema(String name,
        int version,
        MiruFieldDefinition[] fieldDefinitions,
        MiruPropertyDefinition[] propertyDefinitions,
        Map<String, List<String>> pairedLatest,
        Map<String, List<String>> bloom,
        Map<String, String[]> composite,
        Map<String, String[][]> compositePrefixWhitelist,
        ImmutableMap<String, Integer> fieldNameToId,
        ImmutableMap<String, Integer> propNameToId,
        ImmutableList<MiruFieldDefinition>[] fieldToPairedLatestFieldDefinitions,
        ImmutableList<MiruFieldDefinition>[] fieldToBloomFieldDefinitions,
        CompositeFieldDefinition[][] fieldToCompositeDefinitions,
        ImmutableList<Integer> fieldIds,
        ImmutableList<MiruFieldDefinition> fieldsWithFirst,
        ImmutableList<MiruFieldDefinition> fieldsWithLatest,
        ImmutableList<MiruFieldDefinition> fieldsWithPairedLatest,
        ImmutableList<MiruFieldDefinition> fieldsWithBloom) {
        this.name = name;
        this.version = version;
        this.fieldDefinitions = fieldDefinitions;
        this.propertyDefinitions = propertyDefinitions;
        this.pairedLatest = pairedLatest;
        this.bloom = bloom;
        this.composite = composite;
        this.compositePrefixWhitelist = compositePrefixWhitelist;
        this.fieldNameToId = fieldNameToId;
        this.propNameToId = propNameToId;
        this.fieldToPairedLatestFieldDefinitions = fieldToPairedLatestFieldDefinitions;
        this.fieldToBloomFieldDefinitions = fieldToBloomFieldDefinitions;
        this.fieldToCompositeDefinitions = fieldToCompositeDefinitions;
        this.fieldIds = fieldIds;
        this.fieldsWithFirst = fieldsWithFirst;
        this.fieldsWithLatest = fieldsWithLatest;
        this.fieldsWithPairedLatest = fieldsWithPairedLatest;
        this.fieldsWithBloom = fieldsWithBloom;
    }

    @JsonCreator
    public static MiruSchema fromJson(@JsonProperty("name") String name,
        @JsonProperty("version") int version,
        @JsonProperty("fieldDefinitions") MiruFieldDefinition[] fieldDefinitions,
        @JsonProperty("propertyDefinitions") MiruPropertyDefinition[] propertyDefinitions,
        @JsonProperty("pairedLatest") Map<String, List<String>> pairedLatest,
        @JsonProperty("bloom") Map<String, List<String>> bloom,
        @JsonProperty("composite") Map<String, String[]> composite,
        @JsonProperty("compositePrefixWhitelist") Map<String, String[][]> compositePrefixWhitelist) {

        return new Builder(name, version)
            .setFieldDefinitions(fieldDefinitions)
            .setPropertyDefinitions(propertyDefinitions)
            .setPairedLatest(pairedLatest)
            .setBloom(bloom)
            .setComposite(composite)
            .setCompositeWhitelist(compositePrefixWhitelist)
            .build();
    }

    public String getName() {
        return name;
    }

    public int getVersion() {
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

    public Map<String, String[]> getComposite() {
        return composite;
    }

    public Map<String, String[][]> getCompositePrefixWhitelist() {
        return compositePrefixWhitelist;
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
    public List<MiruFieldDefinition> getFieldsWithFirst() {
        return fieldsWithFirst;
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

    @JsonIgnore
    public CompositeFieldDefinition[] getCompositeFieldDefinitions(int fieldId) {
        return fieldToCompositeDefinitions[fieldId];
    }

    public static boolean checkEquals(MiruSchema a, MiruSchema b) {
        return a.version == b.version && a.name.equals(b.name);
    }

    public static boolean deepEquals(MiruSchema a, MiruSchema b) {
        if (a.version == b.version && a.name.equals(b.name)) {
            // Probably incorrect - comparing Object[] arrays with Arrays.equals
            if (!Arrays.equals(a.fieldDefinitions, b.fieldDefinitions)) {
                return false;
            }
            // Probably incorrect - comparing Object[] arrays with Arrays.equals
            if (!Arrays.equals(a.propertyDefinitions, b.propertyDefinitions)) {
                return false;
            }
            if (a.pairedLatest != null ? !a.pairedLatest.equals(b.pairedLatest) : b.pairedLatest != null) {
                return false;
            }
            if (a.bloom != null ? !a.bloom.equals(b.bloom) : b.bloom != null) {
                return false;
            }
            if (!deepEqualsComposite(a.composite, b.composite)) {
                    return false;
            }
            return deepEqualsCompositePrefixWhitelist(a.compositePrefixWhitelist, b.compositePrefixWhitelist);
        }
        return false;
    }

    private static boolean deepEqualsComposite(Map<String, String[]> a, Map<String, String[]> b) {
        if (a == b) {
            return true;
        }

        if (a.size() != b.size()) {
            return false;
        }

        Iterator<Entry<String, String[]>> i = a.entrySet().iterator();
        while (i.hasNext()) {
            Entry<String, String[]> e = i.next();
            String key = e.getKey();
            String[] value = e.getValue();
            if (value == null) {
                if (b.get(key) != null) {
                    return false;
                }
            } else {
                if (!Arrays.equals(value, b.get(key))) {
                    return false;
                }
            }
        }

        return true;
    }

    private static boolean deepEqualsCompositePrefixWhitelist(Map<String, String[][]> a, Map<String, String[][]> b) {
        if (a == b) {
            return true;
        }

        if (a.size() != b.size()) {
            return false;
        }

        Iterator<Entry<String, String[][]>> i = a.entrySet().iterator();
        while (i.hasNext()) {
            Entry<String, String[][]> e = i.next();
            String key = e.getKey();
            String[][] value = e.getValue();
            if (value == null) {
                if (b.get(key) != null) {
                    return false;
                }
            } else {
                if (!Arrays.deepEquals(value, b.get(key))) {
                    return false;
                }
            }
        }

        return true;
    }

    public static boolean checkAdditive(MiruSchema a, MiruSchema b) {
        if (b.fieldDefinitions.length < a.fieldDefinitions.length) {
            return false;
        }
        for (int i = 0; i < a.fieldDefinitions.length; i++) {
            if (!a.fieldDefinitions[i].equals(b.fieldDefinitions[i]) &&
                (!a.name.equals(b.name) || !b.fieldDefinitions[i].type.isAdditiveTo(a.fieldDefinitions[i].type))) {
                return false;
            }
        }
        return true;
    }

    public static class Builder {

        private final String name;
        private final int version;

        private MiruFieldDefinition[] fieldDefinitions = new MiruFieldDefinition[0];
        private MiruPropertyDefinition[] propertyDefinitions = new MiruPropertyDefinition[0];
        private Map<String, List<String>> pairedLatest = Collections.emptyMap();
        private Map<String, List<String>> bloom = Collections.emptyMap();
        private Map<String, String[]> composites = Collections.emptyMap();
        private Map<String, String[][]> compositePrefixWhitelist = Collections.emptyMap();

        public Builder(String name, int version) {
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
            if (composites != null) {
                this.pairedLatest = pairedLatest;
            }
            return this;
        }

        public Builder setBloom(Map<String, List<String>> bloom) {
            if (composites != null) {
                this.bloom = bloom;
            }
            return this;
        }

        public Builder setComposite(Map<String, String[]> composites) {
            if (composites != null) {
                this.composites = composites;
            }
            return this;
        }

        public Builder setCompositeWhitelist(Map<String, String[][]> compositePrefixWhitelist) {
            if (compositePrefixWhitelist != null) {
                this.compositePrefixWhitelist = compositePrefixWhitelist;
            }
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
            CompositeFieldDefinition[][] fieldToCompositeFieldDefinitions = new CompositeFieldDefinition[fieldDefinitions.length][];

            for (MiruFieldDefinition fieldDefinition : fieldDefinitions) {
                fieldNameToId.put(fieldDefinition.name, fieldDefinition.fieldId);
            }

            for (MiruPropertyDefinition propertyDefinition : propertyDefinitions) {
                propNameToId.put(propertyDefinition.name, propertyDefinition.propId);
            }

            ImmutableList.Builder<Integer> fieldIdsBuilder = new ImmutableList.Builder<>();
            ImmutableList.Builder<MiruFieldDefinition> fieldsWithFirstBuilder = new ImmutableList.Builder<>();
            ImmutableList.Builder<MiruFieldDefinition> fieldsWithLatestBuilder = new ImmutableList.Builder<>();
            ImmutableList.Builder<MiruFieldDefinition> fieldsWithPairedLatestBuilder = new ImmutableList.Builder<>();
            ImmutableList.Builder<MiruFieldDefinition> fieldsWithBloomBuilder = new ImmutableList.Builder<>();

            for (MiruFieldDefinition fieldDefinition : fieldDefinitions) {
                fieldIdsBuilder.add(fieldDefinition.fieldId);

                if (fieldDefinition.type.hasFeature(MiruFieldDefinition.Feature.indexedFirst)) {
                    fieldsWithFirstBuilder.add(fieldDefinition);
                }

                if (fieldDefinition.type.hasFeature(MiruFieldDefinition.Feature.indexedLatest)) {
                    fieldsWithLatestBuilder.add(fieldDefinition);
                }

                ImmutableList.Builder<MiruFieldDefinition> pairedLatestFieldDefinitionsBuilder = new ImmutableList.Builder<>();
                List<String> pairedLatestFieldNames = pairedLatest.get(fieldDefinition.name);
                if (pairedLatestFieldNames != null) {
                    fieldsWithPairedLatestBuilder.add(fieldDefinition);
                    for (String pairedLatestFieldName : pairedLatestFieldNames) {
                        MiruFieldDefinition pairedLatestFieldDefinition = fieldDefinitions[fieldNameToId.get(pairedLatestFieldName)];
                        if (pairedLatestFieldDefinition.type.hasFeature(MiruFieldDefinition.Feature.multiValued)) {
                            throw new IllegalArgumentException("Paired latest cannot be applied to multi-term field: "
                                + fieldDefinition.name + " -> " + pairedLatestFieldName);
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

                String[] got = composites.get(fieldDefinition.name);
                String[][] whitelists = compositePrefixWhitelist.get(fieldDefinition.name);
                if (got != null) {
                    CompositeFieldDefinition[] compositeFieldDefinition = new CompositeFieldDefinition[got.length];
                    for (int i = 0; i < got.length; i++) {
                        String[] whitelist = whitelists == null ? null : whitelists[i];
                        compositeFieldDefinition[i] = new CompositeFieldDefinition(fieldDefinitions[fieldNameToId.get(got[i])],
                            whitelist == null ? null : Sets.newHashSet(whitelist));
                    }
                    fieldToCompositeFieldDefinitions[fieldDefinition.fieldId] = compositeFieldDefinition;
                }
            }

            ImmutableList<Integer> fieldIds = fieldIdsBuilder.build();
            ImmutableList<MiruFieldDefinition> fieldsWithFirst = fieldsWithFirstBuilder.build();
            ImmutableList<MiruFieldDefinition> fieldsWithLatest = fieldsWithLatestBuilder.build();
            ImmutableList<MiruFieldDefinition> fieldsWithPairedLatest = fieldsWithPairedLatestBuilder.build();
            ImmutableList<MiruFieldDefinition> fieldsWithBloom = fieldsWithBloomBuilder.build();

            return new MiruSchema(name,
                version,
                fieldDefinitions,
                propertyDefinitions,
                pairedLatest,
                bloom,
                composites,
                compositePrefixWhitelist,
                ImmutableMap.copyOf(fieldNameToId),
                ImmutableMap.copyOf(propNameToId),
                fieldToPairedLatestFieldDefinitions,
                fieldToBloomFieldDefinitions,
                fieldToCompositeFieldDefinitions,
                fieldIds,
                fieldsWithFirst,
                fieldsWithLatest,
                fieldsWithPairedLatest,
                fieldsWithBloom);
        }

    }

    public static class CompositeFieldDefinition {

        final public MiruFieldDefinition definition;
        final public Set<String> whitelist;

        public CompositeFieldDefinition(MiruFieldDefinition definition, Set<String> whitelist) {
            this.definition = definition;
            this.whitelist = whitelist;
        }

    }
}
