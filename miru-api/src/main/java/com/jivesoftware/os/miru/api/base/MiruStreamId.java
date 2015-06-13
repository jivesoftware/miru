package com.jivesoftware.os.miru.api.base;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Only here to make code more readable.
 *
 * @author jonathan
 */
public class MiruStreamId extends MiruIBA {

    public static final MiruStreamId NULL = new MiruStreamId(new byte[0]);

    @JsonCreator
    public MiruStreamId(@JsonProperty("bytes") byte[] _bytes) {
        super(_bytes);
    }
}
