package com.jivesoftware.os.miru.api.wal;

import com.google.common.base.Optional;
import com.google.common.collect.Lists;
import com.google.common.primitives.Longs;
import com.jivesoftware.os.miru.api.topology.NamedCursor;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

/**
 * @author jonathan.colt
 */
public class AmzaCursor implements MiruCursor<AmzaCursor, AmzaSipCursor> {

    public static final AmzaCursor INITIAL = new AmzaCursor(Collections.emptyList(), Optional.<AmzaSipCursor>absent());

    public final List<NamedCursor> cursors;
    public final Optional<AmzaSipCursor> sipCursor;

    public AmzaCursor(Collection<NamedCursor> cursors, Optional<AmzaSipCursor> sipCursor) {
        this.cursors = Lists.newArrayList(cursors);
        Collections.sort(this.cursors);
        this.sipCursor = sipCursor;
    }

    @Override
    public Optional<AmzaSipCursor> getSipCursor() {
        return sipCursor;
    }

    @Override
    public int compareTo(AmzaCursor o) {
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
        return "AmzaCursor{" +
            "cursors=" + cursors +
            '}';
    }
}
