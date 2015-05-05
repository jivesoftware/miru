package com.jivesoftware.os.miru.api.base;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Only here to make code more readable.
 *
 * @author jonathan
 */
public class MiruTermId extends MiruIBA {

    @JsonCreator
    public MiruTermId(@JsonProperty("bytes") byte[] _bytes) {
        super(_bytes);
    }
}
