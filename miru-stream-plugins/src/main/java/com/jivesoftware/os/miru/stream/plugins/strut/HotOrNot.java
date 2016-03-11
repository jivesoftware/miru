package com.jivesoftware.os.miru.stream.plugins.strut;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.jivesoftware.os.miru.api.query.filter.MiruValue;

/**
 *
 * @author jonathan.colt
 */
public class HotOrNot implements Comparable<HotOrNot> {

    public final MiruValue value;
    public final float score;

    @JsonCreator
    public HotOrNot(
        @JsonProperty("value") MiruValue value,
        @JsonProperty("score") float score) {
        this.value = value;
        this.score = score;
    }

    @Override
    public String toString() {
        return "HotOrNot{" + "value=" + value + ", score=" + score + '}';
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
}
