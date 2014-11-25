package com.jivesoftware.os.miru.analytics.plugins.analytics;

import com.fasterxml.jackson.annotation.JsonCreator;
import java.io.Serializable;

/**
 * Requires mapper.configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false);
 */
public class AnalyticsReport implements Serializable {

    @JsonCreator
    public AnalyticsReport() {
    }

    @Override
    public String toString() {
        return "AnalyticsReport{}";
    }
}
