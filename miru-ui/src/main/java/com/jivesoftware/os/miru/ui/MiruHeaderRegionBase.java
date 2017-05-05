package com.jivesoftware.os.miru.ui;

import com.google.common.collect.ImmutableMap;
import com.jivesoftware.os.routing.bird.shared.ConnectionDescriptor;
import com.jivesoftware.os.routing.bird.shared.ConnectionDescriptors;
import com.jivesoftware.os.routing.bird.shared.InstanceDescriptor;
import com.jivesoftware.os.routing.bird.shared.TenantRoutingProvider;
import com.jivesoftware.os.routing.bird.shared.TenantsServiceConnectionDescriptorProvider;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

/**
 * Created by bruce.downs on 5/4/17.
 */
public abstract class MiruHeaderRegionBase implements MiruRegion<Void> {

    protected final TenantRoutingProvider tenantRoutingProvider;

    private String redirUrl;

    public MiruHeaderRegionBase(TenantRoutingProvider tenantRoutingProvider) {
        this.tenantRoutingProvider = tenantRoutingProvider;
    }

    protected int addPeers(List<Map<String, Object>> services, String name, String portName, String path) {
        TenantsServiceConnectionDescriptorProvider readers = tenantRoutingProvider.getConnections(name, portName, 10_000); // TODO config
        @SuppressWarnings("unchecked")
        ConnectionDescriptors connectionDescriptors = readers.getConnections("");

        if (connectionDescriptors != null) {
            List<ConnectionDescriptor> cds = new ArrayList<>(connectionDescriptors.getConnectionDescriptors());
            cds.sort(Comparator.comparingInt((ConnectionDescriptor cd) -> cd.getInstanceDescriptor().instanceName));

            List<Map<String, String>> instances = new ArrayList<>();
            for (ConnectionDescriptor connectionDescriptor : cds) {
                InstanceDescriptor instanceDescriptor = connectionDescriptor.getInstanceDescriptor();
                InstanceDescriptor.InstanceDescriptorPort port = instanceDescriptor.ports.get(portName);
                if (port != null) {
                    instances.add(ImmutableMap.of(
                        "name", instanceDescriptor.serviceName + " " + instanceDescriptor.instanceName,
                        "redirect", redirUrl,
                        "instanceKey", instanceDescriptor.instanceKey,
                        "portName", portName,
                        "path", path));
                }
            }

            if (!instances.isEmpty()) {
                services.add(ImmutableMap.of(
                    "name", name,
                    "instances", instances));
            }

            return instances.size();
        }

        return 0;
    }

    public void setRedirUrl(String redirUrl) { this.redirUrl = redirUrl; }

}
