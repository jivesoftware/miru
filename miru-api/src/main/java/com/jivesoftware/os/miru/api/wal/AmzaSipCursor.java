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

    @JsonCreator
    public AmzaSipCursor(@JsonProperty("cursors") Collection<NamedCursor> cursors) {
        this.cursors = Lists.newArrayList(cursors);
        Collections.sort(this.cursors);
    }

    @Override
    public int compareTo(AmzaSipCursor o) {
        int oSize = o.cursors.size();
        for (int i = 0; i < cursors.size(); i++) {
            long id = cursors.get(i).id;
            long oId = oSize >= i + 1 ? o.cursors.get(i).id : Long.MIN_VALUE;
            int c = Longs.compare(oId, id); // reverse for descending order
            if (c != 0) {
                return c;
            }
        }
        if (oSize > cursors.size()) {
            return 1;
        }
        return 0;
    }

    @Override
    public String toString() {
        return "AmzaSipCursor{" +
            "cursors=" + cursors +
            '}';
    }
}
