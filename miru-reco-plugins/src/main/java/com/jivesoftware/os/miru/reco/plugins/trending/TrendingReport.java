package com.jivesoftware.os.miru.reco.plugins.trending;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.jivesoftware.os.miru.plugin.solution.MiruTimeRange;
import java.io.Serializable;

/**
 *
 */
public class TrendingReport implements Serializable {

    public final MiruTimeRange combinedTimeRange;

    @JsonCreator
    public TrendingReport(@JsonProperty("combinedTimeRange") MiruTimeRange combinedTimeRange) {
        this.combinedTimeRange = combinedTimeRange;
    }
}
