package com.jivesoftware.os.miru.writer.deployable.region;

import com.google.common.collect.Maps;
import com.jivesoftware.os.miru.ui.MiruHeaderRegionBase;
import com.jivesoftware.os.miru.ui.MiruSoyRenderer;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import com.jivesoftware.os.routing.bird.shared.TenantRoutingProvider;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

// soy.miru.chrome.headerRegion
public class MiruHeaderRegion extends MiruHeaderRegionBase {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    private final String cluster;
    private final int instance;
    private final String template;
    private final MiruSoyRenderer renderer;

    public MiruHeaderRegion(String cluster,
        int instance,
        String template,
        MiruSoyRenderer renderer,
        TenantRoutingProvider tenantRoutingProvider) {
        super(tenantRoutingProvider);

        this.cluster = cluster;
        this.instance = instance;
        this.template = template;
        this.renderer = renderer;
    }

    @Override
    public String render(Void input) {
        try {
            Map<String, Object> data = Maps.newHashMap();
            data.put("cluster", cluster);
            data.put("instance", String.valueOf(instance));

            try {
                List<Map<String, Object>> services = new ArrayList<>();
                addPeers(services, "miru-reader", "main", "/ui");
                data.put("total", String.valueOf(addPeers(services, "miru-writer", "main", "/ui")));
                addPeers(services, "miru-manage", "main", "/ui");
                addPeers(services, "miru-wal", "main", "/ui");
                addPeers(services, "miru-catwalk", "main", "/ui");
                addPeers(services, "miru-tools", "main", "/ui");
                data.put("services", services);
            } catch (Exception x) {
                LOG.warn("Failed to build out peers.", x);
            }

            return renderer.render(template, data);
        } catch (Exception x) {
            LOG.error("Failed to render header.", x);
            return x.getMessage();
        }
    }

}
