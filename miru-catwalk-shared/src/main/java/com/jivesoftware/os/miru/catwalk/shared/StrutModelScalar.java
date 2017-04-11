package com.jivesoftware.os.miru.catwalk.shared;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.jivesoftware.os.miru.catwalk.shared.CatwalkQuery.CatwalkModelQuery;
import java.io.Serializable;

/**
 * @author jonathan.colt
 */
public class StrutModelScalar implements Serializable {

    public final String modelId;
    public final CatwalkModelQuery catwalkModelQuery;
    public final float scalar;

    public StrutModelScalar(
        @JsonProperty("modelId") String modelId,
        @JsonProperty("catwalkModelQuery") CatwalkModelQuery catwalkModelQuery,
        @JsonProperty("scalar") float scalar) {
        this.modelId = modelId;
        this.catwalkModelQuery = catwalkModelQuery;
        this.scalar = scalar;
    }

    @Override
    public String toString() {
        return "StrutModelScalar{" +
            "modelId='" + modelId + '\'' +
            ", catwalkModelQuery=" + catwalkModelQuery +
            ", scalar=" + scalar +
            '}';
    }
}
