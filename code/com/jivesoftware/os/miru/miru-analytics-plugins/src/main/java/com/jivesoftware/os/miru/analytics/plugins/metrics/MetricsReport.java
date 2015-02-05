package com.jivesoftware.os.miru.analytics.plugins.metrics;

import com.fasterxml.jackson.annotation.JsonCreator;
import java.io.Serializable;

/**
 * Requires mapper.configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false);
 */
public class MetricsReport implements Serializable {

    @JsonCreator
    public MetricsReport() {
    }

    @Override
    public String toString() {
        return "MetricsReport{}";
    }
}
