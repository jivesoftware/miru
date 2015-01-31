package com.jivesoftware.os.miru.stumptown.plugins;

import com.fasterxml.jackson.annotation.JsonCreator;
import java.io.Serializable;

/**
 * Requires mapper.configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false);
 */
public class StumptownReport implements Serializable {

    @JsonCreator
    public StumptownReport() {
    }

    @Override
    public String toString() {
        return "StumptownReport{}";
    }
}
