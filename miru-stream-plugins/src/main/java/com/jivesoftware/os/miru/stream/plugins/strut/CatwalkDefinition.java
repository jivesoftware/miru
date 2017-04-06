package com.jivesoftware.os.miru.stream.plugins.strut;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.jivesoftware.os.miru.catwalk.shared.CatwalkQuery;
import com.jivesoftware.os.miru.stream.plugins.strut.StrutQuery.Strategy;
import java.io.Serializable;

/**
 *
 */
public class CatwalkDefinition implements Serializable {

    public final String catwalkId;
    public final CatwalkQuery catwalkQuery;
    public final float[] numeratorScalars;
    public final Strategy numeratorStrategy;
    public final float[] featureScalars;
    public final Strategy featureStrategy;

    @JsonCreator
    public CatwalkDefinition(@JsonProperty("catwalkId") String catwalkId,
        @JsonProperty("catwalkQuery") CatwalkQuery catwalkQuery,
        @JsonProperty("numeratorScalars") float[] numeratorScalars,
        @JsonProperty("numeratorStrategy") Strategy numeratorStrategy,
        @JsonProperty("featureScalars") float[] featureScalars,
        @JsonProperty("featureStrategy") Strategy featureStrategy) {
        this.catwalkId = catwalkId;
        this.catwalkQuery = catwalkQuery;
        this.numeratorScalars = numeratorScalars;
        this.numeratorStrategy = numeratorStrategy;
        this.featureScalars = featureScalars;
        this.featureStrategy = featureStrategy;
    }
}
