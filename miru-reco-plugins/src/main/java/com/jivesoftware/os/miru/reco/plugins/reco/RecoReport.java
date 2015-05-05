package com.jivesoftware.os.miru.reco.plugins.reco;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.jivesoftware.os.miru.api.query.filter.MiruFilter;

/**
 *
 */
public class RecoReport {

    public final MiruFilter removeDistinctsFilter;
    public final int collectedDistincts;

    @JsonCreator
    public RecoReport(@JsonProperty("removeDistinctsFilter") MiruFilter removeDistinctsFilter,
        @JsonProperty("collectedDistincts") int collectedDistincts) {
        this.removeDistinctsFilter = removeDistinctsFilter;
        this.collectedDistincts = collectedDistincts;
    }

    @Override
    public String toString() {
        return "RecoReport{" +
            "removeDistinctsFilter=" + removeDistinctsFilter +
            ", collectedDistincts=" + collectedDistincts +
            '}';
    }

}
