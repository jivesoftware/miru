package com.jivesoftware.os.miru.lumberyard.plugins;

import com.fasterxml.jackson.annotation.JsonCreator;
import java.io.Serializable;

/**
 * Requires mapper.configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false);
 */
public class LumberyardReport implements Serializable {

    @JsonCreator
    public LumberyardReport() {
    }

    @Override
    public String toString() {
        return "LumberyardReport{}";
    }
}
