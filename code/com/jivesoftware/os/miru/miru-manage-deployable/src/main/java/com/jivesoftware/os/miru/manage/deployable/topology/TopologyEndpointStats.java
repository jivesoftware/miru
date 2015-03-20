package com.jivesoftware.os.miru.manage.deployable.topology;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 *
 * @author jonathan.colt
 */
public class TopologyEndpointStats {

    private final ConcurrentMap<String, AtomicLong> calledMap = new ConcurrentHashMap<>();

    public Map<String, AtomicLong> calledMap() {
        return calledMap;
    }

    public void called(String path) {
        AtomicLong got = calledMap.get(path);
        if (got == null) {
            got = new AtomicLong();
            AtomicLong had = calledMap.putIfAbsent(path, got);
            if (had != null) {
                got = had;
            }
        }
        got.addAndGet(1);
    }

}
