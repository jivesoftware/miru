package com.jivesoftware.os.miru.stream.plugins.catwalk;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Arrays;
import java.util.List;

/**
 *
 */
public class CatwalkModel {

    public final List<FeatureScore>[] featureScores;

    @JsonCreator
    public CatwalkModel(@JsonProperty("featureScores") List<FeatureScore>[] featureScores) {
        this.featureScores = featureScores;
    }

    @Override
    public String toString() {
        return "CatwalkModel{" +
            "featureScores=" + Arrays.toString(featureScores) +
            '}';
    }

    @Override
    public boolean equals(Object o) {
        throw new UnsupportedOperationException("NOPE");
    }

    @Override
    public int hashCode() {
        throw new UnsupportedOperationException("NOPE");
    }
}
