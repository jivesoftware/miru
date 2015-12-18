package com.jivesoftware.os.miru.reco.plugins.trending;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
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

    public final Map<String, List<Waveform>> waveforms;
    public final Map<String, TrendingAnswerScoreSet> scoreSets;

    @JsonCreator
    public TrendingAnswer(@JsonProperty("waveforms") Map<String, List<Waveform>> waveforms,
        @JsonProperty("scoreSets") Map<String, TrendingAnswerScoreSet> scoreSets) {
        this.waveforms = waveforms;
        this.scoreSets = scoreSets;
    }

    @Override
    public String toString() {
        return "TrendingAnswer{" +
            "waveforms=" + waveforms +
            ", scoreSets=" + scoreSets +
            '}';
    }
}
