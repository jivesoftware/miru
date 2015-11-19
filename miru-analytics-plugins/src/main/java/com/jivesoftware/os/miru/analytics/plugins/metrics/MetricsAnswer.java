package com.jivesoftware.os.miru.analytics.plugins.metrics;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.jivesoftware.os.miru.plugin.solution.Waveform;
import java.io.Serializable;
import java.util.List;

/**
 *
 */
public class MetricsAnswer implements Serializable {

    public static final MetricsAnswer EMPTY_RESULTS = new MetricsAnswer(null,
        true);

    public final List<Waveform> waveforms;
    public final boolean resultsExhausted;

    @JsonCreator
    public MetricsAnswer(
        @JsonProperty("waveforms") List<Waveform> waveforms,
        @JsonProperty("resultsExhausted") boolean resultsExhausted) {
        this.waveforms = waveforms;
        this.resultsExhausted = resultsExhausted;
    }

    @Override
    public String toString() {
        return "MetricsAnswer{"
            + "waveforms=" + waveforms
            + ", resultsExhausted=" + resultsExhausted
            + '}';
    }

}
