package com.jivesoftware.os.miru.sea.anomaly.plugins;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.jivesoftware.os.filer.io.FilerIO;
import com.jivesoftware.os.miru.api.activity.MiruActivity;
import java.io.Serializable;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 *
 */
public class SeaAnomalyAnswer implements Serializable {

    public static final SeaAnomalyAnswer EMPTY_RESULTS = new SeaAnomalyAnswer(null, true);

    public final Map<String, Waveform> waveforms;
    public final boolean resultsExhausted;

    @JsonCreator
    public SeaAnomalyAnswer(
        @JsonProperty("waveforms") Map<String, Waveform> waveforms,
        @JsonProperty("resultsExhausted") boolean resultsExhausted) {
        this.waveforms = waveforms;
        this.resultsExhausted = resultsExhausted;
    }

    @Override
    public String toString() {
        return "StumptownAnswer{" +
            "waveforms=" + waveforms +
            ", resultsExhausted=" + resultsExhausted +
            '}';
    }

    public static class Waveform implements Serializable {

        public final long[] waveform;
        public final List<MiruActivity> results;

        public Waveform(long[] waveform, List<MiruActivity> results) {
            this.waveform = waveform;
            this.results = results;
        }

        @JsonCreator
        public static Waveform fromJson(
            @JsonProperty("waveform") byte[] waveform,
            @JsonProperty("results") List<MiruActivity> results)
            throws Exception {
            return new Waveform(FilerIO.bytesLongs(waveform), results);
        }

        @JsonGetter("waveform")
        public byte[] getTrendAsBytes() throws Exception {
            return FilerIO.longsBytes(waveform);
        }

        @Override
        public String toString() {
            return "Waveform{" +
                "waveform=" + Arrays.toString(waveform) +
                ", results=" + results +
                '}';
        }
    }

}
