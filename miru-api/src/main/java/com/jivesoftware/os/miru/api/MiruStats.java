package com.jivesoftware.os.miru.api;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 *
 * @author jonathan.colt
 */
public class MiruStats {

    private final ConcurrentMap<String, Stat> egressedMap = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, Stat> ingressedMap = new ConcurrentHashMap<>();

    public Map<String, Stat> ingressedMap() {
        return ingressedMap;
    }

    public void ingressed(String tenantId, int count, long latency) {
        Stat got = ingressedMap.get(tenantId);
        if (got == null) {
            got = new Stat();
            Stat had = ingressedMap.putIfAbsent(tenantId, got);
            if (had != null) {
                got = had;
            }
        }
        got.update(count, latency);
    }

    public Map<String, Stat> egressedMap() {
        return egressedMap;
    }

    public void egressed(String path, int count, long latency) {
        Stat got = egressedMap.get(path);
        if (got == null) {
            got = new Stat();
            Stat had = egressedMap.putIfAbsent(path, got);
            if (had != null) {
                got = had;
            }
        }
        got.update(count, latency);
    }

    public static class Stat {

        public final AtomicLong count;
        public final AtomicLong timestamp;
        public final AtomicLong latency;

        Stat() {
            this.count = new AtomicLong();
            this.timestamp = new AtomicLong(System.currentTimeMillis());
            this.latency = new AtomicLong();
        }

        public void update(long amount, long latency) {
            count.addAndGet(amount);
            timestamp.set(System.currentTimeMillis());
            this.latency.set(latency);
        }

    }
}
