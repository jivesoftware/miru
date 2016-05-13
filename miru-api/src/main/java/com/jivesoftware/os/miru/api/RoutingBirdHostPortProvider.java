package com.jivesoftware.os.miru.api;

import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import com.jivesoftware.os.routing.bird.shared.ConnectionDescriptor;
import com.jivesoftware.os.routing.bird.shared.HostPort;
import com.jivesoftware.os.routing.bird.shared.TenantsServiceConnectionDescriptorProvider;
import java.util.List;

/**
 *
 * @author jonathan.colt
 */
public class RoutingBirdHostPortProvider<T> implements HostPortProvider {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();
    private final TenantsServiceConnectionDescriptorProvider<T> connectionDescriptorProvider;
    private final T tenantId;

    public RoutingBirdHostPortProvider(TenantsServiceConnectionDescriptorProvider<T> connectionDescriptorProvider, T tenantId) {
        this.connectionDescriptorProvider = connectionDescriptorProvider;
        this.tenantId = tenantId;
    }

    @Override
    public int getPort(String host) {
        List<ConnectionDescriptor> connectionDescriptors = connectionDescriptorProvider.getConnections(tenantId).getConnectionDescriptors();
        for (ConnectionDescriptor connectionDescriptor : connectionDescriptors) {
            HostPort hostPort = connectionDescriptor.getHostPort();
            if (hostPort.getHost().equals(host)) {
                return hostPort.getPort();
            }
        }
        LOG.warn("Could not find {} in {}", host, connectionDescriptors);
        return -1;
    }

}
