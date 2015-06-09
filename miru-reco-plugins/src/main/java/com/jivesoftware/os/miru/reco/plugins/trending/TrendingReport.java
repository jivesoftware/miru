package com.jivesoftware.os.miru.reco.plugins.trending;

import com.fasterxml.jackson.annotation.JsonCreator;
import java.io.Serializable;

/**
 * Requires mapper.configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false);
 */
public class TrendingReport implements Serializable {

    @JsonCreator
    public TrendingReport() {
    }

    @Override
    public String toString() {
        return "TrendingReport{}";
    }
}
