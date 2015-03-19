package com.jivesoftware.os.miru.writer.deployable.endpoints;

import com.jivesoftware.os.miru.api.activity.MiruActivity;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 *
 * @author jonathan.colt
 */
public class IngressEndpointStats {

    private final ConcurrentMap<MiruTenantId, AtomicLong> ingressedMap = new ConcurrentHashMap<>();

    public Map<MiruTenantId, AtomicLong> ingressedMap() {
        return ingressedMap;
    }

    public void ingressed(MiruTenantId tenantId, int count) {
        AtomicLong got = ingressedMap.get(tenantId);
        if (got == null) {
            got = new AtomicLong();
            AtomicLong had = ingressedMap.putIfAbsent(tenantId, got);
            if (had != null) {
                got = had;
            }
        }
        got.addAndGet(count);
    }

    public void ingressed(List<MiruActivity> activities) {
        for (MiruActivity activity : activities) {
            ingressed(activity.tenantId, 1);
        }
    }

}
