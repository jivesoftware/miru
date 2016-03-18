package com.jivesoftware.os.miru.stream.plugins.strut;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.io.Serializable;

/**
 * @author jonathan
 */
public class StrutReport implements Serializable {

    public final float threshold;

    @JsonCreator
    public StrutReport(@JsonProperty("threshold") float threshold) {
        this.threshold = threshold;
    }

    @Override
    public String toString() {
        return "StrutReport{" +
            "threshold=" + threshold +
            '}';
    }
}
