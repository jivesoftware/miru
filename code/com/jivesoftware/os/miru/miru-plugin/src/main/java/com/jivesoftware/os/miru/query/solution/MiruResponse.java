package com.jivesoftware.os.miru.query.solution;

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
    public final List<String> log;

    @JsonCreator
    public MiruResponse(@JsonProperty("answer") A answer,
            @JsonProperty("solutions") List<MiruSolution> solutions,
            @JsonProperty("totalElapsed") long totalElapsed,
            @JsonProperty("log") List<String> log) {
        this.answer = answer;
        this.solutions = solutions;
        this.totalElapsed = totalElapsed;
        this.log = log;
    }

    @Override
    public String toString() {
        return "MiruResponse{" +
                "answer=" + answer +
                ", solutions=" + solutions +
                ", totalElapsed=" + totalElapsed +
                ", log=" + log +
                '}';
    }
}
