package com.jivesoftware.os.miru.stream.plugins.strut;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.jivesoftware.os.miru.api.query.filter.MiruValue;
import java.io.Serializable;
import java.util.Arrays;
import java.util.List;

/**
 * @author jonathan.colt
 */
public class HotOrNot implements Comparable<HotOrNot>, Serializable {

    public final MiruValue value;
    public final MiruValue[][] gatherLatestValues;
    public final float score;
    public final List<Hotness>[] features;
    public final long timestamp;
    public final boolean unread;
    public final long count;

    @JsonCreator
    public HotOrNot(
        @JsonProperty("value") MiruValue value,
        @JsonProperty("gatherLatestValues") MiruValue[][] gatherLatestValues,
        @JsonProperty("score") float score,
        @JsonProperty("features") List<Hotness>[] features,
        @JsonProperty("timestamp") long timestamp,
        @JsonProperty("unread") boolean unread,
        @JsonProperty("count") long count) {
        this.value = value;
        this.gatherLatestValues = gatherLatestValues;
        this.score = score;
        this.features = features;
        this.timestamp = timestamp;
        this.unread = unread;
        this.count = count;
    }

    @Override
    public String toString() {
        return "HotOrNot{" +
            "value=" + value +
            ", gatherLatestValues=" + Arrays.toString(gatherLatestValues) +
            ", score=" + score +
            ", features=" + Arrays.toString(features) +
            ", timestamp=" + timestamp +
            ", unread=" + unread +
            ", count=" + count +
            '}';
    }

    @Override
    public boolean equals(Object o) {
        throw new UnsupportedOperationException("NOPE");
    }

    @Override
    public int hashCode() {
        throw new UnsupportedOperationException("NOPE");
    }

    @Override
    public int compareTo(HotOrNot o) {
        int c = Float.compare(o.score, score); // reversed
        if (c != 0) {
            return c;
        }
        c = Long.compare(o.timestamp, timestamp); // reversed
        if (c != 0) {
            return c;
        }
        c = Integer.compare(value.parts.length, o.value.parts.length);
        if (c != 0) {
            return c;
        }
        for (int i = 0; i < value.parts.length; i++) {
            c = value.parts[i].compareTo(o.value.parts[i]);
            if (c != 0) {
                return c;
            }
        }
        return c;
    }

    public static class Hotness implements Serializable {

        public final MiruValue[] values;
        public final float scaledScore;
        public final float[] scores;

        @JsonCreator
        public Hotness(@JsonProperty("values") MiruValue[] values,
            @JsonProperty("scaledScore") float scaledScore,
            @JsonProperty("scores") float[] scores) {
            this.values = values;
            this.scaledScore = scaledScore;
            this.scores = scores;
        }

        @Override
        public boolean equals(Object o) {
            throw new UnsupportedOperationException("NOPE");
        }

        @Override
        public int hashCode() {
            throw new UnsupportedOperationException("NOPE");
        }
    }
}
