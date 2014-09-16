package com.jivesoftware.os.miru.reco.plugins.reco;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 *
 */
public class RecoReport {

    public final int collectedDistincts;

    @JsonCreator
    public RecoReport(@JsonProperty("collectedDistincts") int collectedDistincts) {
        this.collectedDistincts = collectedDistincts;
    }

    @Override
    public String toString() {
        return "RecoReport{" + "collectedDistincts=" + collectedDistincts + '}';
    }

}
