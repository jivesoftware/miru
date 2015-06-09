package com.jivesoftware.os.miru.reco.plugins.uniques;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.io.Serializable;

/**
 *
 */
public class UniquesAnswer implements Serializable {

    public static final UniquesAnswer EMPTY_RESULTS = new UniquesAnswer(0, true);

    public final long uniques;
    public final boolean resultsExhausted;

    public UniquesAnswer(
        long uniques,
        boolean resultsExhausted) {
        this.uniques = uniques;
        this.resultsExhausted = resultsExhausted;
    }

    @JsonCreator
    public static UniquesAnswer fromJson(
        @JsonProperty("uniques") long uniques,
        @JsonProperty("resultsExhausted") boolean resultsExhausted) {
        return new UniquesAnswer(uniques, resultsExhausted);
    }

    @Override
    public String toString() {
        return "UniquesAnswer{"
            + "uniques=" + uniques
            + ", resultsExhausted=" + resultsExhausted
            + '}';
    }

}
