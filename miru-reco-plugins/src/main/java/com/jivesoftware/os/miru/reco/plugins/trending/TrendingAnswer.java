package com.jivesoftware.os.miru.reco.plugins.trending;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.jivesoftware.os.miru.api.query.filter.MiruValue;
import com.jivesoftware.os.miru.plugin.solution.Waveform;
import java.io.Serializable;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 *
 */
public class TrendingAnswer implements Serializable {

    public static final TrendingAnswer EMPTY_RESULTS = new TrendingAnswer(Collections.emptyMap(), Collections.emptyMap());

    public final Map<MiruValue, Waveform> waveforms;
    public final Map<String, List<Trendy>> results;

    @JsonCreator
    public TrendingAnswer(@JsonProperty("waveforms") Map<MiruValue, Waveform> waveforms,
        @JsonProperty("results") Map<String, List<Trendy>> results) {
        this.waveforms = waveforms;
        this.results = results;
    }

    @Override
    public String toString() {
        return "TrendingAnswer{"
            + "waveforms=" + waveforms
            + "results=" + results
            + '}';
    }
}
