package com.jivesoftware.os.miru.api.base;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.jivesoftware.os.jive.utils.id.Id;

/**
 * Only here to make code more readable.
 *
 * @author jonathan
 */
public class MiruStreamId extends MiruIBA {

    @JsonCreator
    public MiruStreamId(@JsonProperty("bytes") byte[] _bytes) {
        super(_bytes);
    }

    @Override
    public String toString() {
        return new Id(getBytes()).toStringForm();
    }
}
