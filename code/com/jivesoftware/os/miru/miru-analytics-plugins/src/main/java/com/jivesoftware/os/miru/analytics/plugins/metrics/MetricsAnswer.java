package com.jivesoftware.os.miru.analytics.plugins.metrics;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.jivesoftware.os.filer.io.FilerIO;
import java.io.Serializable;
import java.util.Arrays;
import java.util.Map;

/**
 *
 */
public class MetricsAnswer implements Serializable {

    public static final MetricsAnswer EMPTY_RESULTS = new MetricsAnswer(null,
        true);

    public final Map<String, Waveform> waveforms;
    public final boolean resultsExhausted;

    @JsonCreator
    public MetricsAnswer(
        @JsonProperty("waveforms") Map<String, Waveform> waveforms,
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

    public static class Waveform implements Serializable {

        public final long[] waveform;

        public Waveform(long[] waveform) {
            this.waveform = waveform;
        }

        @JsonCreator
        public static Waveform fromJson(
            @JsonProperty("waveform") byte[] waveform)
            throws Exception {
            return new Waveform(FilerIO.bytesLongs(waveform));
        }

        @JsonGetter("waveform")
        public byte[] getTrendAsBytes() throws Exception {
            return FilerIO.longsBytes(waveform);
        }

        @Override
        public String toString() {
            return "Waveform{"
                + "waveform=" + Arrays.toString(waveform)
                + '}';
        }
    }

}
