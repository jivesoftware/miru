package com.jivesoftware.os.miru.catwalk.shared;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.io.Serializable;

/**
 *
 * @author jonathan.colt
 */
public class StrutModelScalar implements Serializable {

    public final String catwalkId;
    public final String modelId;
    public final CatwalkQuery catwalkQuery;
    public final float scalar;

    public StrutModelScalar(
        @JsonProperty("catwalkId") String catwalkId,
        @JsonProperty("modelId") String modelId,
        @JsonProperty("catwalkQuery") CatwalkQuery catwalkQuery,
        @JsonProperty("scalar") float scalar) {
        this.catwalkId = catwalkId;
        this.modelId = modelId;
        this.catwalkQuery = catwalkQuery;
        this.scalar = scalar;
    }

    @Override
    public String toString() {
        return "StrutConstraint{" + "catwalkId=" + catwalkId + ", modelId=" + modelId + ", catwalkQuery=" + catwalkQuery + ", scalar=" + scalar + '}';
    }


}
