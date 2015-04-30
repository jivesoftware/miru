package com.jivesoftware.os.miru.api.topology;

import java.util.Collection;

/**
 *
 */
public class NamedCursorsResult<T> {

    public Collection<NamedCursor> cursors;
    public T result;

    public NamedCursorsResult() {
    }

    public NamedCursorsResult(Collection<NamedCursor> cursors, T result) {
        this.cursors = cursors;
        this.result = result;
    }
}
