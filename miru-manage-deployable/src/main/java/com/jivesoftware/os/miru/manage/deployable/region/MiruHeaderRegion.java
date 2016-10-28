package com.jivesoftware.os.miru.manage.deployable.region;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.jivesoftware.os.miru.ui.MiruRegion;
import com.jivesoftware.os.miru.ui.MiruSoyRenderer;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import com.jivesoftware.os.routing.bird.shared.ConnectionDescriptor;
import com.jivesoftware.os.routing.bird.shared.ConnectionDescriptors;
import com.jivesoftware.os.routing.bird.shared.InstanceDescriptor;
import com.jivesoftware.os.routing.bird.shared.TenantRoutingProvider;
import com.jivesoftware.os.routing.bird.shared.TenantsServiceConnectionDescriptorProvider;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

// soy.miru.chrome.headerRegion
public class MiruHeaderRegion implements MiruRegion<Void> {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();
    private final String cluster;
    private final int instance;
    private final String template;
    private final MiruSoyRenderer renderer;
    private final TenantRoutingProvider tenantRoutingProvider;

    public MiruHeaderRegion(String cluster,
        int instance,
        String template,
        MiruSoyRenderer renderer,
        TenantRoutingProvider tenantRoutingProvider) {
        this.cluster = cluster;
        this.instance = instance;
        this.template = template;
        this.renderer = renderer;
        this.tenantRoutingProvider = tenantRoutingProvider;
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
                addPeers(services, "miru-writer", "main", "/ui");
                addPeers(services, "miru-wal", "main", "/ui");
                data.put("total", String.valueOf(addPeers(services, "miru-manage", "main", "/ui")));
                addPeers(services, "miru-catwalk", "main", "/ui");
                addPeers(services, "miru-tools", "main", "/ui");
                data.put("services", services);

            } catch (Exception x) {
                LOG.warn("Failed to build out peers.", x);
            }

            return renderer.render(template, data);
        } catch (Exception x) {
            LOG.error("Faild to render header.", x);
            return x.getMessage();
        }
    }

    private int addPeers(List<Map<String, Object>> services, String name, String portName, String path) {
        if (tenantRoutingProvider != null) {
            TenantsServiceConnectionDescriptorProvider readers = tenantRoutingProvider.getConnections(name, portName, 10_000); // TODO config
            ConnectionDescriptors connectionDescriptors = readers.getConnections("");
            if (connectionDescriptors != null) {
                List<Map<String, String>> instances = new ArrayList<>();
                List<ConnectionDescriptor> cds = new ArrayList<>(connectionDescriptors.getConnectionDescriptors());
                Collections.sort(cds, (ConnectionDescriptor o1, ConnectionDescriptor o2) -> {
                    return Integer.compare(o1.getInstanceDescriptor().instanceName, o2.getInstanceDescriptor().instanceName);
                });
                for (ConnectionDescriptor connectionDescriptor : cds) {
                    InstanceDescriptor instanceDescriptor = connectionDescriptor.getInstanceDescriptor();
                    InstanceDescriptor.InstanceDescriptorPort port = instanceDescriptor.ports.get(portName);
                    if (port != null) {
                        instances.add(ImmutableMap.of("name", instanceDescriptor.serviceName + " " + instanceDescriptor.instanceName,
                            "host", instanceDescriptor.publicHost,
                            "port", String.valueOf(port.port),
                            "path", path
                        ));

                    }
                }
                if (!instances.isEmpty()) {
                    services.add(ImmutableMap.of(
                        "name", name,
                        "instances", instances));
                }
                return instances.size();
            }
        }
        return 0;
    }
}
