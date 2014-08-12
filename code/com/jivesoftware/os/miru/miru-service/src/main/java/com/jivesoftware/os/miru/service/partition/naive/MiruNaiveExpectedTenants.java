package com.jivesoftware.os.miru.service.partition.naive;

import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.service.partition.MiruExpectedTenants;
import com.jivesoftware.os.miru.service.partition.MiruTenantTopology;
import com.jivesoftware.os.miru.service.partition.MiruTenantTopologyFactory;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import javax.inject.Inject;
import javax.inject.Singleton;

/**
 * You get a topology, and you get a topology, and you get a topology! Everybody gets a topology!
 */
@Singleton
public class MiruNaiveExpectedTenants implements MiruExpectedTenants {

    private final MiruTenantTopologyFactory tenantTopologyFactory;

    private final ConcurrentHashMap<MiruTenantId, MiruTenantTopology> topology = new ConcurrentHashMap<>();

    @Inject
    public MiruNaiveExpectedTenants(MiruTenantTopologyFactory tenantTopologyFactory) {
        this.tenantTopologyFactory = tenantTopologyFactory;
    }

    @Override
    public MiruTenantTopology getTopology(MiruTenantId tenantId) {
        MiruTenantTopology tenantTopology = topology.get(tenantId);
        if (tenantTopology == null) {
            topology.putIfAbsent(tenantId, tenantTopologyFactory.create(tenantId));
            tenantTopology = topology.get(tenantId);
        }
        return tenantTopology;
    }

    @Override
    public Collection<MiruTenantTopology> topologies() {
        return topology.values();
    }

    @Override
    public boolean isExpected(MiruTenantId tenantId) {
        return true;
    }

    @Override
    public void expect(List<MiruTenantId> expectedTenantsForHost) {
        // we'll create topologies for these on the fly
    }
}
