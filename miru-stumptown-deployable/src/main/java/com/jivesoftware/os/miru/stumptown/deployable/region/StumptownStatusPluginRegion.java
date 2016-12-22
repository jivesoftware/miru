package com.jivesoftware.os.miru.stumptown.deployable.region;

import com.google.common.collect.Maps;
import com.jivesoftware.os.miru.stumptown.deployable.LogMill;
import com.jivesoftware.os.miru.stumptown.deployable.ServiceId;
import com.jivesoftware.os.miru.ui.MiruPageRegion;
import com.jivesoftware.os.miru.ui.MiruSoyRenderer;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 *
 */
// soy.stumptown.page.stumptownStatusPluginRegion
public class StumptownStatusPluginRegion implements MiruPageRegion<StumptownStatusPluginRegion.StumptownStatusPluginRegionInput> {

    private static final MetricLogger log = MetricLoggerFactory.getLogger();

    private final String template;
    private final MiruSoyRenderer renderer;
    private final LogMill logMill;

    public StumptownStatusPluginRegion(String template,
        MiruSoyRenderer renderer,
        LogMill logMill) {
        this.template = template;
        this.renderer = renderer;
        this.logMill = logMill;
    }

    public static class StumptownStatusPluginRegionInput {

        final String action;

        public StumptownStatusPluginRegionInput(String action) {

            this.action = action;
        }

    }

    @Override
    public String render(StumptownStatusPluginRegionInput input) {
        Map<String, Object> data = Maps.newHashMap();

        try {

            if (input.action.equals("reset")) {
                Map<ServiceId, Map<String, AtomicLong>> rowMap = logMill.levelCounts.rowMap();
                for (Map<String, AtomicLong> map : rowMap.values()) {
                    for (AtomicLong atomicLong : map.values()) {
                        atomicLong.set(0);
                    }
                }
            }

            List<Map<String, String>> rows = new ArrayList<>();
            Map<ServiceId, Map<String, AtomicLong>> rowMap = logMill.levelCounts.rowMap();
            for (ServiceId serviceId : rowMap.keySet()) {

                Map<String, String> status = new HashMap<>();
                status.put("cluster", serviceId.cluster);
                status.put("host", serviceId.host);
                status.put("service", serviceId.service);
                status.put("instance", serviceId.instance);
                status.put("version", serviceId.version);

                Map<String, AtomicLong> levelCounts = rowMap.get(serviceId);
                for (String level : new String[] { "info", "warn", "error" }) {
                    if (levelCounts.containsKey(level.toUpperCase())) {
                        status.put(level, String.valueOf(levelCounts.get(level.toUpperCase()).get()));
                    } else {
                        status.put(level, String.valueOf(0L));
                    }
                }
                rows.add(status);

            }

            Collections.sort(rows, (o1, o2) -> {
                String a = o1.get("error");
                String b = o2.get("error");
                if (a != null && b != null) {
                    int i = -Long.compare(Long.parseLong(a), Long.parseLong(b));
                    if (i != 0) {
                        return i;
                    }
                }
                a = o1.get("warn");
                b = o2.get("warn");
                if (a != null && b != null) {
                    int i = -Long.compare(Long.parseLong(a), Long.parseLong(b));
                    if (i != 0) {
                        return i;
                    }
                }
                a = o1.get("info");
                b = o2.get("info");
                if (a != null && b != null) {
                    int i = -Long.compare(Long.parseLong(a), Long.parseLong(b));
                    if (i != 0) {
                        return i;
                    }
                }
                return -1;
            });

            data.put("serviceStatus", rows);


        } catch (Exception e) {
            log.error("Unable to retrieve data", e);
        }

        return renderer.render(template, data);
    }

    @Override
    public String getTitle() {
        return "Status";
    }

    static class ServiceStatus {

    }
}
