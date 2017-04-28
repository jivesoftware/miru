package com.jivesoftware.os.miru.siphon.deployable.siphoner;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.jivesoftware.os.amza.api.ring.RingMember;
import java.util.Map;

/**
 * Created by jonathan.colt on 4/27/17.
 */
public class AmzaSiphonCursor {

    public final Map<RingMember, Long> memberTxIds;

    @JsonCreator
    public AmzaSiphonCursor(
        @JsonProperty("memberTxIds") Map<RingMember, Long> memberTxIds) {

        this.memberTxIds = memberTxIds;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof AmzaSiphonCursor)) {
            return false;
        }

        AmzaSiphonCursor that = (AmzaSiphonCursor) o;

        return memberTxIds.equals(that.memberTxIds);

    }

    @Override
    public int hashCode() {
        return memberTxIds.hashCode();
    }
}
