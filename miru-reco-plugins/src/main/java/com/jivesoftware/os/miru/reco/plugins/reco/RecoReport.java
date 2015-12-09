package com.jivesoftware.os.miru.reco.plugins.reco;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.jivesoftware.os.miru.api.query.filter.MiruValue;
import java.util.List;

/**
 *
 */
public class RecoReport {

    public final List<MiruValue> removeDistincts;
    public final int collectedDistincts;

    @JsonCreator
    public RecoReport(@JsonProperty("removeDistinctsFilter") List<MiruValue> removeDistincts,
        @JsonProperty("collectedDistincts") int collectedDistincts) {
        this.removeDistincts = removeDistincts;
        this.collectedDistincts = collectedDistincts;
    }

    @Override
    public String toString() {
        return "RecoReport{" +
            "removeDistincts=" + removeDistincts +
            ", collectedDistincts=" + collectedDistincts +
            '}';
    }

}
