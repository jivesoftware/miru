package com.jivesoftware.os.miru.lumberyard.deployable;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 *
 */
public class FreshCutTimber {

    public final String message;
    public final String[] thrownStackTrace;

    @JsonCreator
    public FreshCutTimber(@JsonProperty("message") String message,
        @JsonProperty("thrownStackTrace") String[] thrownStackTrace) {
        this.message = message;
        this.thrownStackTrace = thrownStackTrace;
    }
}
