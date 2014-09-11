package com.jivesoftware.os.miru.service.partition;

import com.jivesoftware.os.miru.api.MiruHost;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.query.bitmap.MiruBitmapsProvider;
import com.jivesoftware.os.miru.service.MiruServiceConfig;

/**
 *
 */
public class MiruTenantTopologyFactory {

    private final MiruServiceConfig config;
    private final MiruBitmapsProvider bitmapsProvider;
    private final MiruHost localHost;
    private final MiruLocalPartitionFactory localPartitionFactory;
    private final MiruRemotePartitionFactory remotePartitionFactory;
    private final MiruHostedPartitionComparison partitionComparison;

    public MiruTenantTopologyFactory(MiruServiceConfig config, MiruBitmapsProvider bitmapsProvider, MiruHost localHost,
            MiruLocalPartitionFactory localPartitionFactory, MiruRemotePartitionFactory remotePartitionFactory,
            MiruHostedPartitionComparison partitionComparison) {
        this.config = config;
        this.bitmapsProvider = bitmapsProvider;
        this.localHost = localHost;
        this.localPartitionFactory = localPartitionFactory;
        this.remotePartitionFactory = remotePartitionFactory;
        this.partitionComparison = partitionComparison;
    }

    public MiruTenantTopology<?> create(MiruTenantId tenantId) {
        return new MiruTenantTopology<>(config, bitmapsProvider.getBitmaps(tenantId), localHost, tenantId, localPartitionFactory, remotePartitionFactory,
                partitionComparison);
    }
}
