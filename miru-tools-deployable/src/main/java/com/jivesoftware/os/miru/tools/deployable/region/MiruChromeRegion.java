package com.jivesoftware.os.miru.tools.deployable.region;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.jivesoftware.os.miru.ui.MiruPageRegion;
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

/**
 *
 */
// soy.miru.chrome.chromeRegion
public class MiruChromeRegion<I, R extends MiruPageRegion<I>> implements MiruRegion<I> {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    private final String template;
    private final MiruSoyRenderer renderer;
    private final MiruHeaderRegion headerRegion;
    private final List<MiruToolsPlugin> plugins;
    private final R region;
    private final TenantRoutingProvider tenantRoutingProvider;

    public MiruChromeRegion(String template, MiruSoyRenderer renderer, MiruHeaderRegion headerRegion, List<MiruToolsPlugin> plugins, R region,
        TenantRoutingProvider tenantRoutingProvider) {
        this.template = template;
        this.renderer = renderer;
        this.headerRegion = headerRegion;
        this.plugins = plugins;
        this.region = region;
        this.tenantRoutingProvider = tenantRoutingProvider;
    }

    @Override
    public String render(I input) {

        Map<String, Object> headerData = Maps.newHashMap();
        try {

            List<Map<String, Object>> services = new ArrayList<>();
            addPeers(services, "miru-reader", "main", "/");
            addPeers(services, "miru-writer", "main", "/miru/writer");
            addPeers(services, "miru-manage", "main", "/miru/manage");
            addPeers(services, "miru-tools", "main", "/");
            headerData.put("services", services);

        } catch (Exception x) {
            LOG.warn("Failed to build out peers.", x);
        }

        Map<String, Object> data = Maps.newHashMap();
        data.put("header", headerRegion.render(null));
        data.put("region", region.render(input));
        data.put("title", region.getTitle());
        data.put("plugins", Lists.transform(plugins,
            plugin -> ImmutableMap.of("glyphicon", plugin.glyphicon, "name", plugin.name, "path", plugin.path)));
        return renderer.render(template, data);

    }

    private void addPeers(List<Map<String, Object>> services, String name, String portName, String path) {
        TenantsServiceConnectionDescriptorProvider readers = tenantRoutingProvider.getConnections(name, portName);
        ConnectionDescriptors connectionDescriptors = readers.getConnections("");
        if (connectionDescriptors != null) {
            List<Map<String, String>> instances = new ArrayList<>();
            for (ConnectionDescriptor connectionDescriptor : connectionDescriptors.getConnectionDescriptors()) {
                InstanceDescriptor instanceDescriptor = connectionDescriptor.getInstanceDescriptor();
                InstanceDescriptor.InstanceDescriptorPort port = instanceDescriptor.ports.get(portName);
                if (port == null) {
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
