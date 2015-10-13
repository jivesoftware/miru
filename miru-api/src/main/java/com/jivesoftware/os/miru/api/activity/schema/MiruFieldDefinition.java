package com.jivesoftware.os.miru.api.activity.schema;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableSet;
import java.util.Set;

/**
 *
 */
public class MiruFieldDefinition {

    public final int fieldId;
    public final String name;
    public final Type type;
    public final Prefix prefix;

    @JsonCreator
    public MiruFieldDefinition(@JsonProperty("fieldId") int fieldId,
        @JsonProperty("name") String name,
        @JsonProperty("type") Type type,
        @JsonProperty("prefix") Prefix prefix) {

        this.fieldId = fieldId;
        this.name = name;
        this.type = type;
        this.prefix = prefix;
    }

    public enum Type {
        singleTerm(Feature.indexed),
        singleTermIndexLatest(Feature.indexed, Feature.indexedLatest),
        multiTerm(Feature.indexed, Feature.multiValued),
        multiTermCardinality(Feature.indexed, Feature.multiValued, Feature.cardinality),
        nonIndexed();

        private final Set<Feature> features;

        Type(Feature... features) {
            this.features = ImmutableSet.copyOf(features);
        }

        public boolean hasFeature(Feature feature) {
            return features.contains(feature);
        }
    }

    public enum Feature {
        indexed,
        indexedLatest,
        multiValued,
        cardinality;
    }

    /**
     * An optional field prefix.
     * <p>
     * If of type {@link Type#none}, then the other parameters are ignored (use {@link #NONE}).
     * <p>
     * If of type {@link Type#raw}, then 1 byte of the given length is reserved for the number of bytes used by the prefix.
     * <p>
     * If of type {@link Type#numeric}, then a numeric string is converted to its lexicographical byte representation.
     * Acceptable numeric lengths are 4 (int) and 8 (long).
     */
    public static class Prefix {

        public static final Prefix NONE = new Prefix(Type.none, 0, '\0');
        public static final Prefix WILDCARD = new Prefix(Type.wildcard, 0, '\0');

        public final Type type;
        public final int length;
        public final int separator;

        @JsonCreator
        public Prefix(@JsonProperty("type") Type type,
            @JsonProperty("length") int length,
            @JsonProperty("separator") int separator) {
            this.type = type;
            this.length = length;
            this.separator = separator;
        }

        public enum Type {
            none(false),
            wildcard(false),
            raw(true),
            numeric(true);

            private final boolean analyzed;

            Type(boolean analyzed) {
                this.analyzed = analyzed;
            }

            public boolean isAnalyzed() {
                return analyzed;
            }
        }
    }
}
