package com.jivesoftware.os.miru.plugin.solution;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.io.Serializable;
import java.util.List;

/**
 * @author jonathan.colt
 */
public class MiruPartitionResponse<A> implements Serializable {

    public final A answer;
    public final List<String> log;

    @JsonCreator
    public MiruPartitionResponse(@JsonProperty("answer") A answer,
            @JsonProperty("log") List<String> log) {
        this.answer = answer;
        this.log = log;
    }

}
