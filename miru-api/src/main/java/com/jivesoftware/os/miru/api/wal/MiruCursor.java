package com.jivesoftware.os.miru.api.wal;

import com.google.common.base.Optional;

/**
 *
 */
public interface MiruCursor<C, S extends MiruSipCursor<S>> extends Comparable<C> {

    Optional<S> getSipCursor();
}
