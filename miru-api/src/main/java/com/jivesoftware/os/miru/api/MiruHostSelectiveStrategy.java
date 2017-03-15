package com.jivesoftware.os.miru.api;

import com.jivesoftware.os.routing.bird.shared.ClientCall;
import com.jivesoftware.os.routing.bird.shared.ClientHealth;
import com.jivesoftware.os.routing.bird.shared.ConnectionDescriptor;
import com.jivesoftware.os.routing.bird.shared.HostPort;
import com.jivesoftware.os.routing.bird.shared.HttpClientException;
import com.jivesoftware.os.routing.bird.shared.IndexedClientStrategy;
import com.jivesoftware.os.routing.bird.shared.InstanceDescriptor;
import com.jivesoftware.os.routing.bird.shared.NextClientStrategy;
import com.jivesoftware.os.routing.bird.shared.ReturnFirstNonFailure;

import java.util.Arrays;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

public class MiruHostSelectiveStrategy implements NextClientStrategy, IndexedClientStrategy {

    private final MiruHost[] orderHosts;
    private final AtomicReference<DescriptorsReference> lastDescriptorsReference = new AtomicReference<>();
    private final ReturnFirstNonFailure returnFirstNonFailure = new ReturnFirstNonFailure();

    public MiruHostSelectiveStrategy(MiruHost[] orderHosts) {
        this.orderHosts = orderHosts;
    }

    @Override
    public <C, R> R call(String family,
        ClientCall<C, R, HttpClientException> httpCall,
        ConnectionDescriptor[] connectionDescriptors,
        long connectionDescriptorsVersion, C[] clients,
        ClientHealth[] clientHealths,
        int deadAfterNErrors,
        long checkDeadEveryNMillis,
        AtomicInteger[] clientsErrors,
        AtomicLong[] clientsDeathTimestamp) throws HttpClientException {
        return returnFirstNonFailure.call(this,
            family,
            httpCall,
            connectionDescriptors,
            connectionDescriptorsVersion,
            clients,
            clientHealths,
            deadAfterNErrors,
            checkDeadEveryNMillis,
            clientsErrors,
            clientsDeathTimestamp);
    }

    @Override
    public int[] getClients(ConnectionDescriptor[] connectionDescriptors) {
        DescriptorsReference descriptorsReference = lastDescriptorsReference.get();
        if (descriptorsReference != null) {
            if (descriptorsReference.connectionDescriptors == connectionDescriptors) {
                return descriptorsReference.indexes;
            }
        }

        int[] indexes = new int[orderHosts.length];
        Arrays.fill(indexes, -1);
        for (int i = 0; i < orderHosts.length; i++) {
            for (int j = 0; j < connectionDescriptors.length; j++) {
                InstanceDescriptor instanceDescriptor = connectionDescriptors[j].getInstanceDescriptor();
                HostPort hostPort = connectionDescriptors[j].getHostPort();
                if (MiruHostProvider.checkEquals(orderHosts[i],
                    instanceDescriptor.instanceName, instanceDescriptor.instanceKey,
                    hostPort.getHost(), hostPort.getPort())) {
                    indexes[i] = j;
                    break;
                }
            }
        }
        lastDescriptorsReference.set(new DescriptorsReference(connectionDescriptors, indexes));
        return indexes;
    }

    @Override
    public void usedClientAtIndex(int index) {
    }

    @Override
    public String toString() {
        return "MiruHostSelectiveStrategy{"
            + "orderHosts=" + Arrays.toString(orderHosts)
            + ", lastDescriptorsReference=" + lastDescriptorsReference
            + '}';
    }

    private static class DescriptorsReference {

        private final ConnectionDescriptor[] connectionDescriptors;
        private final int[] indexes;

        public DescriptorsReference(ConnectionDescriptor[] connectionDescriptors, int[] indexes) {
            this.connectionDescriptors = connectionDescriptors;
            this.indexes = indexes;
        }
    }
}
