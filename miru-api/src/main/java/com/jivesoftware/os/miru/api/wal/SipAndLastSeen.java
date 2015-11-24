package com.jivesoftware.os.miru.api.wal;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.jivesoftware.os.miru.api.activity.TimeAndVersion;
import java.util.Set;

/**
 *
 */
public class SipAndLastSeen<S extends MiruSipCursor> {

    public final S sipCursor;
    public final Set<TimeAndVersion> lastSeen;

    @JsonCreator
    public SipAndLastSeen(@JsonProperty("sipCursor") S sipCursor,
        @JsonProperty("lastSeen") Set<TimeAndVersion> lastSeen) {
        this.sipCursor = sipCursor;
        this.lastSeen = lastSeen;
    }

    @Override
    public String toString() {
        return "SipAndLastSeen{" +
            "sipCursor=" + sipCursor +
            ", lastSeen=" + lastSeen +
            '}';
    }
}
