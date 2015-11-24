package com.jivesoftware.os.miru.api.wal;

/**
 *
 */
public interface MiruSipCursor<S> extends Comparable<S> {

    boolean endOfStream();
}
