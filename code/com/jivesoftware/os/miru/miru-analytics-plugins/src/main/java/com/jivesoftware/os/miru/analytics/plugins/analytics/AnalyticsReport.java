package com.jivesoftware.os.miru.analytics.plugins.analytics;

import com.fasterxml.jackson.annotation.JsonCreator;
import java.io.Serializable;

/**
 *
 */
public class AnalyticsReport implements Serializable {

    public AnalyticsReport() {
    }

    @JsonCreator
    public static AnalyticsReport fromJson() {
        return new AnalyticsReport();
    }

    @Override
    public String toString() {
        return "AnalyticsReport{}";
    }
}
