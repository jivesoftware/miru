package com.jivesoftware.os.miru.query;

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

    @JsonCreator
    public MiruResponse(@JsonProperty("answer") A answer,
            @JsonProperty("solutions") List<MiruSolution> solutions,
            @JsonProperty("totalElapsed") long totalElapsed) {
        this.answer = answer;
        this.solutions = solutions;
        this.totalElapsed = totalElapsed;
    }

    @Override
    public String toString() {
        return "MiruResponse{" +
                "answer=" + answer +
                ", solutions=" + solutions +
                ", totalElapsed=" + totalElapsed +
                '}';
    }
}
