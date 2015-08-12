package com.jivesoftware.os.miru.api.wal;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.Lists;
import com.google.common.primitives.Longs;
import com.jivesoftware.os.miru.api.topology.NamedCursor;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

/**
 * @author jonathan.colt
 */
public class AmzaSipCursor implements MiruSipCursor<AmzaSipCursor> {

    public final List<NamedCursor> cursors;
    public final boolean endOfStream;

    @JsonCreator
    public AmzaSipCursor(@JsonProperty("cursors") Collection<NamedCursor> cursors,
        @JsonProperty("endOfStream") boolean endOfStream) {
        this.cursors = Lists.newArrayList(cursors);
        Collections.sort(this.cursors);
        this.endOfStream = endOfStream;
    }

    @Override
    public boolean endOfStream() {
        return endOfStream;
    }

    @Override
    public int compareTo(AmzaSipCursor o) {
        int oSize = o.cursors.size();
        for (int i = 0; i < cursors.size(); i++) {
            long id = cursors.get(i).id;
            long oId = (i < oSize) ? o.cursors.get(i).id : Long.MIN_VALUE;
            int c = Long.compare(id, oId);
            if (c != 0) {
                return c;
            }
        }
        int c = Integer.compare(cursors.size(), oSize);
        if (c != 0) {
            return c;
        }
        return Boolean.compare(endOfStream, o.endOfStream);
    }

    @Override
    public String toString() {
        return "AmzaSipCursor{" +
            "cursors=" + cursors +
            ", endOfStream=" + endOfStream +
            '}';
    }
}
