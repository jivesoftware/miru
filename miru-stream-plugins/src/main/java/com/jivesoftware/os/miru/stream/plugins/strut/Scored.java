package com.jivesoftware.os.miru.stream.plugins.strut;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.jivesoftware.os.miru.api.base.MiruTermId;
import com.jivesoftware.os.miru.stream.plugins.strut.HotOrNot.Hotness;
import java.io.Serializable;
import java.util.List;

/**
 *
 */
public class Scored implements Comparable<Scored>, Serializable {

    public final int lastId;
    public final MiruTermId term;
    public final int scoredToLastId;
    public final float scaledScore;
    public final float[] scores;
    public final List<Hotness>[] features;
    public final long count;

    @JsonCreator
    public Scored(@JsonProperty("lastId") int lastId,
        @JsonProperty("term") MiruTermId term,
        @JsonProperty("scoredToLastId") int scoredToLastId,
        @JsonProperty("scaledScore") float scaledScore,
        @JsonProperty("scores") float[] scores,
        @JsonProperty("features") List<Hotness>[] features,
        @JsonProperty("count") long count) {

        this.lastId = lastId;
        this.term = term;
        this.scaledScore = scaledScore;
        this.scoredToLastId = scoredToLastId;
        this.scores = scores;
        this.features = features;
        this.count = count;
    }

    @Override
    public int compareTo(Scored o) {
        int c = Float.compare(o.scaledScore, scaledScore); // reversed
        if (c != 0) {
            return c;
        }
        c = Integer.compare(o.lastId, lastId); // reversed
        if (c != 0) {
            return c;
        }
        return term.compareTo(o.term);
    }

}
