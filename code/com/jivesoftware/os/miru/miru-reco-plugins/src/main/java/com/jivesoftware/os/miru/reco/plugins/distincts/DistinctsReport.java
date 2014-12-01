package com.jivesoftware.os.miru.reco.plugins.distincts;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.io.Serializable;

/**
 *
 */
public class DistinctsReport implements Serializable {

    public final int collectedDistincts;

    @JsonCreator
    public DistinctsReport(@JsonProperty("collectedDistincts") int collectedDistincts) {
        this.collectedDistincts = collectedDistincts;
    }

    @Override
    public String toString() {
        return "DistinctsReport{" +
            ", collectedDistincts=" + collectedDistincts +
            '}';
    }
}
