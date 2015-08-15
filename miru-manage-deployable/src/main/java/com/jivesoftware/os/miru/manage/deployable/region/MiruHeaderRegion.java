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
                addPeers(services, "miru-reader", "main", "/");
                addPeers(services, "miru-writer", "main", "/miru/writer");
                addPeers(services, "miru-manage", "main", "/miru/manage");
                addPeers(services, "miru-tools", "main", "/");
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

    private void addPeers(List<Map<String, Object>> services, String name, String portName, String path) {
        if (tenantRoutingProvider != null) {
            TenantsServiceConnectionDescriptorProvider readers = tenantRoutingProvider.getConnections(name, portName);
            ConnectionDescriptors connectionDescriptors = readers.getConnections("");
            if (connectionDescriptors != null) {
                List<Map<String, String>> instances = new ArrayList<>();
                for (ConnectionDescriptor connectionDescriptor : connectionDescriptors.getConnectionDescriptors()) {
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
            }
        }
    }
}
