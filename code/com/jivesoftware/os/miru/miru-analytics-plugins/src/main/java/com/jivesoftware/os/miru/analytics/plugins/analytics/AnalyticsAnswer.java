package com.jivesoftware.os.miru.analytics.plugins.analytics;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.jivesoftware.os.filer.io.FilerIO;
import java.io.Serializable;
import java.util.Arrays;

/**
 *
 */
public class AnalyticsAnswer implements Serializable {

    public static final AnalyticsAnswer EMPTY_RESULTS = new AnalyticsAnswer(null,
        true);

    public final Waveform waveform;
    public final boolean resultsExhausted;

    public AnalyticsAnswer(
        Waveform waveform,
        boolean resultsExhausted) {
        this.waveform = waveform;
        this.resultsExhausted = resultsExhausted;
    }

    @JsonCreator
    public static AnalyticsAnswer fromJson(
        @JsonProperty("waveform") Waveform waveform,
        @JsonProperty("resultsExhausted") boolean resultsExhausted) {
        return new AnalyticsAnswer(waveform, resultsExhausted);
    }

    @Override
    public String toString() {
        return "AnalyticsAnswer{"
            + "waveform=" + waveform
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
