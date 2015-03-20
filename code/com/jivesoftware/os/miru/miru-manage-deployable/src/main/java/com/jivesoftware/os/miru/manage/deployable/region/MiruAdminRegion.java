package com.jivesoftware.os.miru.manage.deployable.region;

import com.google.common.collect.Maps;
import com.jivesoftware.os.miru.manage.deployable.MiruSoyRenderer;
import com.jivesoftware.os.miru.manage.deployable.topology.TopologyEndpointStats;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 *
 */
public class MiruAdminRegion implements MiruPageRegion<Void> {

    private final String template;
    private final MiruSoyRenderer renderer;
    private final TopologyEndpointStats stats;

    public MiruAdminRegion(String template, MiruSoyRenderer renderer, TopologyEndpointStats stats) {
        this.template = template;
        this.renderer = renderer;
        this.stats = stats;
    }

    @Override
    public String render(Void input) {
        Map<String, Object> data = Maps.newHashMap();

        List<Map<String, String>> rows = new ArrayList<>();
        long grandTotal = 0;
        Map<String, AtomicLong> calledMap = stats.calledMap();
        List<Map.Entry<String, AtomicLong>> sortedEntries = new ArrayList<>(calledMap.entrySet());

        Collections.sort(sortedEntries,
            new Comparator<Map.Entry<String, AtomicLong>>() {

                @Override
                public int compare(Map.Entry<String, AtomicLong> o1, Map.Entry<String, AtomicLong> o2) {
                    return Long.compare(o2.getValue().longValue(), o1.getValue().longValue());
                }
            }
        );

        for (Map.Entry<String, AtomicLong> e : sortedEntries) {
            Map<String, String> status = new HashMap<>();
            status.put("context", e.getKey());
            status.put("called", String.valueOf(e.getValue().get()));
            rows.add(status);
            grandTotal += e.getValue().get();
        }
        data.put("calledTotal", String.valueOf(grandTotal));
        data.put("calledStatus", rows);

        return renderer.render(template, data);
    }

    @Override
    public String getTitle() {
        return "Status";
    }
}
