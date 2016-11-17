package com.jivesoftware.os.wiki.miru.deployable.region;

import com.google.common.collect.Maps;
import com.jivesoftware.os.miru.ui.MiruPageRegion;
import com.jivesoftware.os.miru.ui.MiruSoyRenderer;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import com.jivesoftware.os.wiki.miru.deployable.WikiMiruStressService;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;

/**
 *
 */
public class WikiMiruStressPluginRegion implements MiruPageRegion<WikiMiruStressPluginRegion.WikiMiruStressPluginRegionInput> {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    private final String template;
    private final MiruSoyRenderer renderer;
    private final WikiMiruStressService stressService;
    private final Map<String, WikiMiruStressService.Stresser> stressers = new ConcurrentHashMap<>();

    public WikiMiruStressPluginRegion(String template,
        MiruSoyRenderer renderer,
        WikiMiruStressService stressService) {
        this.template = template;
        this.renderer = renderer;
        this.stressService = stressService;
    }

    public static class WikiMiruStressPluginRegionInput {

        final String stresserId;
        final String tenantId;
        final int qps;
        final String action;

        public WikiMiruStressPluginRegionInput(String stresserId, String tenantId, int batchSize, String action) {
            this.stresserId = stresserId;
            this.tenantId = tenantId;
            this.qps = batchSize;
            this.action = action;
        }

    }

    @Override
    public String render(WikiMiruStressPluginRegionInput input) {
        Map<String, Object> data = Maps.newHashMap();
        try {

            if (input.action.equals("start")) {
                WikiMiruStressService.Stresser s = stressService.stress(input.tenantId, input.qps);
                stressers.put(s.stresserId, s);
                Executors.newSingleThreadExecutor().submit(() -> {
                    try {
                        s.start();
                        return null;
                    } catch (Throwable x) {
                        s.message = "failed: "+x.getMessage();
                        LOG.error("Wiki oops", x);
                        return null;
                    }
                });
            }

            if (input.action.equals("stop")) {
                WikiMiruStressService.Stresser s = stressers.get(input.stresserId);
                if (s != null) {
                    s.running.set(false);
                }
            }

            if (input.action.equals("remove")) {
                WikiMiruStressService.Stresser s = stressers.remove(input.stresserId);
                if (s != null) {
                    s.running.set(false);
                }
            }

            List<Map<String, String>> rows = new ArrayList<>();
            for (WikiMiruStressService.Stresser s : stressers.values()) {
                Map<String, String> m = new HashMap<>();
                m.put("message", s.message);
                m.put("stresserId", s.stresserId);
                m.put("running", s.running.toString());
                m.put("queried", s.queried.toString());
                m.put("tenantId", s.tenantId);
                m.put("elapse", String.valueOf(System.currentTimeMillis() - s.startTimestampMillis));

                rows.add(m);
            }
            data.put("stressers", rows);

        } catch (Exception e) {
            LOG.error("Unable to retrieve data", e);
        }

        return renderer.render(template, data);
    }

    @Override
    public String getTitle() {
        return "Wiki Stresser";
    }

}
