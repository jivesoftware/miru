package com.jivesoftware.os.miru.sync.api;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;

/**
 * Created by jonathan.colt on 1/13/17.
 */
public class MiruSyncTenantTuple {
    public final MiruTenantId from;
    public final MiruTenantId to;

    @JsonCreator
    public MiruSyncTenantTuple(@JsonProperty("from") MiruTenantId from,
        @JsonProperty("to") MiruTenantId to) {

        Preconditions.checkNotNull(from);
        Preconditions.checkNotNull(to);

        this.from = from;
        this.to = to;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof MiruSyncTenantTuple)) {
            return false;
        }

        MiruSyncTenantTuple that = (MiruSyncTenantTuple) o;

        if (!from.equals(that.from)) {
            return false;
        }
        return to.equals(that.to);

    }

    @Override
    public int hashCode() {
        int result = from.hashCode();
        result = 31 * result + to.hashCode();
        return result;
    }

    public static byte[] toBytes(MiruSyncTenantTuple tuple) {
        return toKeyString(tuple).getBytes(StandardCharsets.UTF_8);
    }

    public static String toKeyString(MiruSyncTenantTuple tuple) {
        return tuple.from.toString() + " " + tuple.to.toString();
    }

    public static MiruSyncTenantTuple fromBytes(byte[] bytes) throws InterruptedException {
        return fromKeyString(new String(bytes, StandardCharsets.UTF_8));
    }

    public static MiruSyncTenantTuple fromKeyString(String key) throws InterruptedException {
        Iterable<String> fromTo = Splitter.on(' ').split(key);
        Iterator<String> iterator = fromTo.iterator();
        MiruTenantId from = iterator.hasNext() ? new MiruTenantId(iterator.next().getBytes(StandardCharsets.UTF_8)) : null;
        MiruTenantId to = iterator.hasNext() ? new MiruTenantId(iterator.next().getBytes(StandardCharsets.UTF_8)) : null;
        return new MiruSyncTenantTuple(from, to);
    }
}
