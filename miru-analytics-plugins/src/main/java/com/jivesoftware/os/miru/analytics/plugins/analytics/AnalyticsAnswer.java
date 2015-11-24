package com.jivesoftware.os.miru.analytics.plugins.analytics;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.jivesoftware.os.miru.plugin.solution.Waveform;
import java.io.Serializable;
import java.util.List;

/**
 *
 */
public class AnalyticsAnswer implements Serializable {

    public static final AnalyticsAnswer EMPTY_RESULTS = new AnalyticsAnswer(null,
        true);

    public final List<Waveform> waveforms;
    public final boolean resultsExhausted;

    @JsonCreator
    public AnalyticsAnswer(
        @JsonProperty("waveforms") List<Waveform> waveforms,
        @JsonProperty("resultsExhausted") boolean resultsExhausted) {
        this.waveforms = waveforms;
        this.resultsExhausted = resultsExhausted;
    }

    @Override
    public String toString() {
        return "AnalyticsAnswer{"
            + "waveforms=" + waveforms
            + ", resultsExhausted=" + resultsExhausted
            + '}';
    }

}
