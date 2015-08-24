package com.jivesoftware.os.miru.sea.anomaly.plugins;

import com.fasterxml.jackson.annotation.JsonCreator;
import java.io.Serializable;

/**
 * Requires mapper.configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false);
 */
public class SeaAnomalyReport implements Serializable {

    @JsonCreator
    public SeaAnomalyReport() {
    }

    @Override
    public String toString() {
        return "AnomalyReport{}";
    }
}
