package com.jivesoftware.os.miru.anomaly.plugins;

import com.fasterxml.jackson.annotation.JsonCreator;
import java.io.Serializable;

/**
 * Requires mapper.configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false);
 */
public class AnomalyReport implements Serializable {

    @JsonCreator
    public AnomalyReport() {
    }

    @Override
    public String toString() {
        return "AnomalyReport{}";
    }
}
