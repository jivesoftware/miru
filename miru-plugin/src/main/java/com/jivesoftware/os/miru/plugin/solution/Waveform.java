package com.jivesoftware.os.miru.plugin.solution;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.jivesoftware.os.filer.io.FilerIO;
import java.io.Serializable;
import java.util.Arrays;

/**
 *
 */
public class Waveform implements Serializable {

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
