package com.jivesoftware.os.miru.plugin.solution;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;

/**
 *
 */
public class MiruResponse<A> {

    public final A answer;
    public final List<MiruSolution> solutions;
    public final long totalElapsed;
    public final boolean notRegistered;
    public final List<String> log;

    @JsonCreator
    public MiruResponse(@JsonProperty("answer") A answer,
        @JsonProperty("solutions") List<MiruSolution> solutions,
        @JsonProperty("totalElapsed") long totalElapsed,
        @JsonProperty("notRegistered") boolean notRegistered,
        @JsonProperty("log") List<String> log) {
        this.answer = answer;
        this.solutions = solutions;
        this.totalElapsed = totalElapsed;
        this.notRegistered = notRegistered;
        this.log = log;
    }

    @Override
    public String toString() {
        return "MiruResponse{" +
            "answer=" + answer +
            ", solutions=" + solutions +
            ", totalElapsed=" + totalElapsed +
            ", notRegistered=" + notRegistered +
            ", log=" + log +
            '}';
    }
}
