package com.jivesoftware.os.miru.service.partition;

import com.jivesoftware.os.miru.api.base.MiruTenantId;
import java.util.Collection;
import java.util.List;

/**
 * To expect a tenant is to consider it active, meaning its partitions are eligible to wake and serve activity.
 *
 * To host a tenant means at least one partition for the tenant is expected to be replicated.
 *
 * The topology for a tenant is the complete breakdown of hosts and partitions.
 */
public interface MiruExpectedTenants {

    MiruTenantTopology<?> getTopology(MiruTenantId tenantId);

    Collection<MiruTenantTopology<?>> topologies();

    boolean isExpected(MiruTenantId tenantId);

    void expect(List<MiruTenantId> expectedTenantsForHost) throws Exception;
}
