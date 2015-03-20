package com.jivesoftware.os.miru.writer.deployable.region;

import com.google.common.collect.Maps;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.writer.deployable.MiruSoyRenderer;
import com.jivesoftware.os.miru.writer.deployable.endpoints.IngressEndpointStats;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicLong;

/**
 *
 */
public class MiruAdminRegion implements MiruPageRegion<Void> {

    private final String template;
    private final MiruSoyRenderer renderer;
    private final IngressEndpointStats endpointStats;

    public MiruAdminRegion(String template, MiruSoyRenderer renderer, IngressEndpointStats endpointStats) {
        this.template = template;
        this.renderer = renderer;
        this.endpointStats = endpointStats;
    }

    @Override
    public String render(Void input) {

        Map<String, Object> data = Maps.newHashMap();

        List<Map<String, String>> rows = new ArrayList<>();
        long grandTotal = 0;
        Map<MiruTenantId, AtomicLong> ingressedMap = endpointStats.ingressedMap();
        List<Entry<MiruTenantId, AtomicLong>> sortedEntries = new ArrayList<>(ingressedMap.entrySet());

        Collections.sort(sortedEntries,
            new Comparator<Entry<MiruTenantId, AtomicLong>>() {

                @Override
                public int compare(Entry<MiruTenantId, AtomicLong> o1, Entry<MiruTenantId, AtomicLong> o2) {
                    return Long.compare(o2.getValue().longValue(), o1.getValue().longValue());
                }
            }
        );

        for (Map.Entry<MiruTenantId, AtomicLong> e : sortedEntries) {
            Map<String, String> status = new HashMap<>();
            status.put("tenantId", e.getKey().toString());
            status.put("ingressed", String.valueOf(e.getValue().get()));
            rows.add(status);
            grandTotal += e.getValue().get();
        }
        data.put("ingressedTotal", String.valueOf(grandTotal));
        data.put("ingressedStatus", rows);

        return renderer.render(template, data);
    }

    @Override
    public String getTitle() {
        return "Status";
    }
}
