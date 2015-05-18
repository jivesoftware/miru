package com.jivesoftware.os.miru.api.wal;

/**
 *
 */
public interface MiruCursor<C, S extends MiruSipCursor<S>> extends Comparable<C> {

    S getSipCursor();
}
