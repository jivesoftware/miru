package com.jivesoftware.os.miru.logappender;

import com.jivesoftware.os.routing.bird.shared.ConnectionDescriptor;
import com.jivesoftware.os.routing.bird.shared.ConnectionDescriptors;
import com.jivesoftware.os.routing.bird.shared.HostPort;
import com.jivesoftware.os.routing.bird.shared.TenantsServiceConnectionDescriptorProvider;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 *
 * @author jonathan.colt
 */
public class RoutingBirdLogSenderProvider<T> implements MiruLogSenderProvider {

    private final AtomicLong lastCachedTimestamp = new AtomicLong(-1);
    private final AtomicReference<MiruLogSender[]> cache = new AtomicReference<>(null);

    private final TenantsServiceConnectionDescriptorProvider<T> connections;
    private final T tenant;
    private final long soTimeout;

    public RoutingBirdLogSenderProvider(TenantsServiceConnectionDescriptorProvider<T> connections, T tenant, long soTimeout) {
        this.connections = connections;
        this.tenant = tenant;
        this.soTimeout = soTimeout;
    }

    @Override
    public MiruLogSender[] getLogSenders() {
        try {
            ConnectionDescriptors connectionDescriptors = connections.getConnections(tenant);
            if (connectionDescriptors.getTimestamp() > lastCachedTimestamp.get()) {
                List<ConnectionDescriptor> latest = connectionDescriptors.getConnectionDescriptors();
                MiruLogSender[] senders = new MiruLogSender[latest.size()];
                int i = 0;
                for (ConnectionDescriptor connectionDescriptor : latest) {
                    HostPort hostPort = connectionDescriptor.getHostPort();
                    senders[i] = new NettyHttpPoster(hostPort.getHost(), hostPort.getPort(), soTimeout);
                    i++;
                }
                cache.set(senders);
                lastCachedTimestamp.set(connectionDescriptors.getTimestamp());
                return senders;
            } else {
                return cache.get();
            }
        } catch (Exception x) {
            return new MiruLogSender[0];
        }
    }

}
