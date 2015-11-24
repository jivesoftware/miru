package com.jivesoftware.os.miru.reco.plugins.trending;

import com.jivesoftware.os.miru.plugin.solution.Waveform;

/**
 *
 */
public class TrendingVersionedWaveform {
    final long version;
    final Waveform waveform;

    TrendingVersionedWaveform(long version, Waveform waveform) {
        this.version = version;
        this.waveform = waveform;
    }
}
