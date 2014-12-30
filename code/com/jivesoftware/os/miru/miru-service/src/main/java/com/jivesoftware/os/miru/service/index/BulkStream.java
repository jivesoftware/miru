package com.jivesoftware.os.miru.service.index;

/**
 *
 */
public interface BulkStream<V> {

    boolean stream(V v) throws Exception;
}
