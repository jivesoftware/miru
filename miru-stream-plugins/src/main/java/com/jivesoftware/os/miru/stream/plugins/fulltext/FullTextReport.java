package com.jivesoftware.os.miru.stream.plugins.fulltext;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.io.Serializable;


/**
 * @author jonathan
 */
public class FullTextReport implements Serializable {

    public final int scoredActivities;
    public final float lowestScore;
    public final float highestScore;

    public FullTextReport(
        @JsonProperty("scoredActivities") int scoredActivities,
        @JsonProperty("lowestScore") float lowestScore,
        @JsonProperty("highestScore") float highestScore) {
        this.scoredActivities = scoredActivities;
        this.lowestScore = lowestScore;
        this.highestScore = highestScore;
    }

    @Override
    public String toString() {
        return "FullTextReport{" +
            "scoredActivities=" + scoredActivities +
            ", lowestScore=" + lowestScore +
            ", highestScore=" + highestScore +
            '}';
    }
}
