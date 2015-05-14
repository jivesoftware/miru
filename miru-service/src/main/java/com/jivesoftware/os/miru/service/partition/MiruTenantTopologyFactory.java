package com.jivesoftware.os.miru.service.partition;

import com.jivesoftware.os.miru.api.MiruHost;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.plugin.bitmap.MiruBitmapsProvider;
import com.jivesoftware.os.miru.service.MiruServiceConfig;
import com.jivesoftware.os.miru.service.partition.cluster.MiruTenantTopology;

/**
 *
 */
public class MiruTenantTopologyFactory {

    private final MiruServiceConfig config;
    private final MiruBitmapsProvider bitmapsProvider;
    private final MiruHost localHost;
    private final MiruLocalPartitionFactory<?, ?> localPartitionFactory;

    public MiruTenantTopologyFactory(MiruServiceConfig config, MiruBitmapsProvider bitmapsProvider, MiruHost localHost,
        MiruLocalPartitionFactory<?, ?> localPartitionFactory) {
        this.config = config;
        this.bitmapsProvider = bitmapsProvider;
        this.localHost = localHost;
        this.localPartitionFactory = localPartitionFactory;
    }

    public MiruTenantTopology<?> create(MiruTenantId tenantId) {
        return new MiruTenantTopology<>(config.getEnsurePartitionsIntervalInMillis(),
            bitmapsProvider.getBitmaps(tenantId),
            localHost,
            tenantId,
            localPartitionFactory);
    }

    public void prioritizeRebuild(MiruLocalHostedPartition<?, ?, ?> partition) {
        localPartitionFactory.prioritizeRebuild(partition);
    }
}
