package com.jivesoftware.os.miru.cluster;

import com.google.inject.AbstractModule;
import com.google.inject.TypeLiteral;
import com.google.inject.name.Names;
import com.jivesoftware.os.miru.api.MiruHost;
import com.jivesoftware.os.miru.api.activity.MiruPartitionId;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.cluster.rcvs.MiruActivityLookupEntry;
import com.jivesoftware.os.miru.cluster.rcvs.MiruHostsColumnKey;
import com.jivesoftware.os.miru.cluster.rcvs.MiruHostsColumnValue;
import com.jivesoftware.os.miru.cluster.rcvs.MiruRCVSActivityLookupTable;
import com.jivesoftware.os.miru.cluster.rcvs.MiruTopologyColumnKey;
import com.jivesoftware.os.miru.cluster.rcvs.MiruTopologyColumnValue;
import com.jivesoftware.os.miru.cluster.rcvs.MiruVoidByte;
import com.jivesoftware.os.jive.utils.row.column.value.store.api.RowColumnValueStore;

import static com.google.common.base.Preconditions.checkNotNull;

public class MiruRegistryInitializerModule extends AbstractModule {

    private final MiruRegistryConfig config;

    private final RowColumnValueStore<MiruVoidByte, MiruHost, MiruHostsColumnKey, MiruHostsColumnValue, ? extends Exception> hostsRegistry;
    private final RowColumnValueStore<MiruVoidByte, MiruHost, MiruTenantId, MiruVoidByte, ? extends Exception> expectedTenantsRegistry;
    private final RowColumnValueStore<MiruTenantId, MiruHost, MiruPartitionId, MiruVoidByte, ? extends Exception> expectedTenantPartitionsRegistry;
    private final RowColumnValueStore<MiruTenantId, MiruPartitionId, Long, MiruHost, ? extends Exception> replicaRegistry;
    private final RowColumnValueStore<MiruVoidByte, MiruTenantId, MiruTopologyColumnKey, MiruTopologyColumnValue, ? extends Exception> topologyRegistry;
    private final RowColumnValueStore<MiruVoidByte, MiruTenantId, MiruTenantConfigFields, Long, ? extends Exception> configRegistry;
    private final RowColumnValueStore<MiruVoidByte, MiruTenantId, Integer, MiruPartitionId, ? extends Exception> writerPartitionRegistry;
    private final RowColumnValueStore<MiruVoidByte, MiruTenantId, Long, MiruActivityLookupEntry, ? extends Exception> activityLookupTable;

    private final Class<? extends MiruClusterRegistry> clusterRegistryClass;

    public MiruRegistryInitializerModule(
        MiruRegistryConfig config,
        RowColumnValueStore<MiruVoidByte, MiruHost, MiruHostsColumnKey, MiruHostsColumnValue, ? extends Exception> hostsRegistry,
        RowColumnValueStore<MiruVoidByte, MiruHost, MiruTenantId, MiruVoidByte, ? extends Exception> expectedTenantsRegistry,
        RowColumnValueStore<MiruTenantId, MiruHost, MiruPartitionId, MiruVoidByte, ? extends Exception> expectedTenantPartitionsRegistry,
        RowColumnValueStore<MiruTenantId, MiruPartitionId, Long, MiruHost, ? extends Exception> replicaRegistry,
        RowColumnValueStore<MiruVoidByte, MiruTenantId, MiruTopologyColumnKey, MiruTopologyColumnValue, ? extends Exception> topologyRegistry,
        RowColumnValueStore<MiruVoidByte, MiruTenantId, MiruTenantConfigFields, Long, ? extends Exception> configRegistry,
        RowColumnValueStore<MiruVoidByte, MiruTenantId, Integer, MiruPartitionId, ? extends Exception> writerPartitionRegistry,
        RowColumnValueStore<MiruVoidByte, MiruTenantId, Long, MiruActivityLookupEntry, ? extends Exception> activityLookupTable,
        Class<? extends MiruClusterRegistry> clusterRegistryClass) {

        this.config = config;

        this.hostsRegistry = checkNotNull(hostsRegistry);
        this.expectedTenantsRegistry = checkNotNull(expectedTenantsRegistry);
        this.expectedTenantPartitionsRegistry = checkNotNull(expectedTenantPartitionsRegistry);
        this.replicaRegistry = checkNotNull(replicaRegistry);
        this.topologyRegistry = checkNotNull(topologyRegistry);
        this.configRegistry = checkNotNull(configRegistry);
        this.writerPartitionRegistry = checkNotNull(writerPartitionRegistry);
        this.activityLookupTable = checkNotNull(activityLookupTable);

        this.clusterRegistryClass = checkNotNull(clusterRegistryClass);
    }

    @Override
    protected void configure() {
        bind(MiruRegistryConfig.class).toInstance(config);

        bindConstant().annotatedWith(Names.named("miruNumberOfReplicas")).to(config.getDefaultNumberOfReplicas());
        bindConstant().annotatedWith(Names.named("miruTopologyIsStaleAfterMillis")).to(config.getDefaultTopologyIsStaleAfterMillis());

        bind(new TypeLiteral<RowColumnValueStore<MiruVoidByte, MiruHost, MiruHostsColumnKey, MiruHostsColumnValue, ? extends Exception>>() {
        }).toInstance(hostsRegistry);
        bind(new TypeLiteral<RowColumnValueStore<MiruVoidByte, MiruHost, MiruTenantId, MiruVoidByte, ? extends Exception>>() {
        }).toInstance(expectedTenantsRegistry);
        bind(new TypeLiteral<RowColumnValueStore<MiruTenantId, MiruHost, MiruPartitionId, MiruVoidByte, ? extends Exception>>() {
        }).toInstance(expectedTenantPartitionsRegistry);
        bind(new TypeLiteral<RowColumnValueStore<MiruTenantId, MiruPartitionId, Long, MiruHost, ? extends Exception>>() {
        }).toInstance(replicaRegistry);
        bind(new TypeLiteral<RowColumnValueStore<MiruVoidByte, MiruTenantId, MiruTopologyColumnKey, MiruTopologyColumnValue, ? extends Exception>>() {
        }).toInstance(topologyRegistry);
        bind(new TypeLiteral<RowColumnValueStore<MiruVoidByte, MiruTenantId, MiruTenantConfigFields, Long, ? extends Exception>>() {
        }).toInstance(configRegistry);
        bind(new TypeLiteral<RowColumnValueStore<MiruVoidByte, MiruTenantId, Integer, MiruPartitionId, ? extends Exception>>() {
        }).toInstance(writerPartitionRegistry);
        bind(new TypeLiteral<RowColumnValueStore<MiruVoidByte, MiruTenantId, Long, MiruActivityLookupEntry, ? extends Exception>>() {
        }).toInstance(activityLookupTable);

        bind(MiruClusterRegistry.class).to(clusterRegistryClass);
        bind(MiruActivityLookupTable.class).to(MiruRCVSActivityLookupTable.class);
    }

}
