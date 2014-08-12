package com.jivesoftware.os.miru.cluster;

import com.google.inject.Guice;
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.jivesoftware.os.miru.api.MiruHost;
import com.jivesoftware.os.miru.api.activity.MiruPartitionId;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.cluster.marshaller.MiruActivityLookupEntryMarshaller;
import com.jivesoftware.os.miru.cluster.marshaller.MiruHostMarshaller;
import com.jivesoftware.os.miru.cluster.marshaller.MiruHostsColumnKeyMarshaller;
import com.jivesoftware.os.miru.cluster.marshaller.MiruHostsColumnValueMarshaller;
import com.jivesoftware.os.miru.cluster.marshaller.MiruPartitionIdMarshaller;
import com.jivesoftware.os.miru.cluster.marshaller.MiruTenantConfigFieldsMarshaller;
import com.jivesoftware.os.miru.cluster.marshaller.MiruTenantIdMarshaller;
import com.jivesoftware.os.miru.cluster.marshaller.MiruTopologyColumnKeyMarshaller;
import com.jivesoftware.os.miru.cluster.marshaller.MiruTopologyColumnValueMarshaller;
import com.jivesoftware.os.miru.cluster.marshaller.MiruVoidByteMarshaller;
import com.jivesoftware.os.miru.cluster.rcvs.MiruActivityLookupEntry;
import com.jivesoftware.os.miru.cluster.rcvs.MiruHostsColumnKey;
import com.jivesoftware.os.miru.cluster.rcvs.MiruHostsColumnValue;
import com.jivesoftware.os.miru.cluster.rcvs.MiruTopologyColumnKey;
import com.jivesoftware.os.miru.cluster.rcvs.MiruTopologyColumnValue;
import com.jivesoftware.os.miru.cluster.rcvs.MiruVoidByte;
import com.jivesoftware.os.jive.utils.id.SaltingDelegatingMarshaller;
import com.jivesoftware.os.jive.utils.row.column.value.store.api.DefaultRowColumnValueStoreMarshaller;
import com.jivesoftware.os.jive.utils.row.column.value.store.api.RowColumnValueStore;
import com.jivesoftware.os.jive.utils.row.column.value.store.api.SetOfSortedMapsImplInitializer;
import com.jivesoftware.os.jive.utils.row.column.value.store.api.timestamper.CurrentTimestamper;
import com.jivesoftware.os.jive.utils.row.column.value.store.marshall.primatives.IntegerTypeMarshaller;
import com.jivesoftware.os.jive.utils.row.column.value.store.marshall.primatives.LongTypeMarshaller;

/**
 * miru.reg.h [Void, Host] [h=hosts, e=expectedTenants]
 *
 * - h: Heartbeat = void (timestamp for recency)
 *
 * - e: Tenant1 = void
 * - e: Tenant2 = void
 * - e: Tenant3 = void
 *
 * miru.reg.th [Tenant, Host] [e=expectedTenantPartitions]
 *
 * - e: Partition1 = void
 * - e: Partition2 = void
 * - e: Partition3 = void
 *
 * miru.reg.tp [Tenant, Partition] [r=replica]
 *
 * TODO could be - r: Host1 = 1 (timestamp for recency)
 * - r: InverseDateAdded1 = Host1
 * - r: InverseDateAdded2 = Host2
 * - r: InverseDateAdded3 = Host3
 *
 * miru.reg.t [Void, Tenant] [t=topology, c=config, p=partition]
 *
 * TODO store sip time - t: Partition1 Host1 = State BackingStorage SipTimestamp (timestamp for staleness)
 * - t: Partition1 Host1 = State BackingStorage (timestamp for staleness)
 * - t: Partition1 Host2 = State BackingStorage (timestamp for staleness)
 * - t: Partition1 Host3 = State BackingStorage (timestamp for staleness)
 * - t: Partition2 Host1 = State BackingStorage (timestamp for staleness)
 * - t: Partition2 Host2 = State BackingStorage (timestamp for staleness)
 * - t: Partition2 Host3 = State BackingStorage (timestamp for staleness)
 *
 * - c: NumberOfReplicas           = 3
 * - c: TopologyIsStaleAfterMillis = 1_000_000
 *
 * - p: WriterId = versioned PartitionId (partitionIdX, partitionIdX-1, ...)
 */
public class MiruRegistryInitializer {

    public static MiruRegistryInitializer initialize(
        MiruRegistryConfig config,
        String tableNameSpace,
        SetOfSortedMapsImplInitializer<? extends Exception> setOfSortedMapsImplInitializer,
        Class<? extends MiruClusterRegistry> clusterRegistryClass) throws Exception {

        // Miru Hosts Registry
        RowColumnValueStore<MiruVoidByte, MiruHost, MiruHostsColumnKey, MiruHostsColumnValue, ? extends Exception> hostsRegistry =
            setOfSortedMapsImplInitializer.initialize(
                tableNameSpace,
                "miru.reg.h", // Host
                "h",
                new String[] { "e" },
                new DefaultRowColumnValueStoreMarshaller<>(
                        new MiruVoidByteMarshaller(),
                        new SaltingDelegatingMarshaller<>(new MiruHostMarshaller()),
                        new MiruHostsColumnKeyMarshaller(),
                        new MiruHostsColumnValueMarshaller()),
                new CurrentTimestamper()
            );

        // Miru Expected Tenants Registry
        RowColumnValueStore<MiruVoidByte, MiruHost, MiruTenantId, MiruVoidByte, ? extends Exception> expectedTenantsRegistry =
            setOfSortedMapsImplInitializer.initialize(
                tableNameSpace,
                "miru.reg.h", // Host
                "e",
                new String[] { "h" },
                new DefaultRowColumnValueStoreMarshaller<>(
                        new MiruVoidByteMarshaller(),
                        new SaltingDelegatingMarshaller<>(new MiruHostMarshaller()),
                        new MiruTenantIdMarshaller(),
                        new MiruVoidByteMarshaller()),
                new CurrentTimestamper()
            );

        // Miru Expected Tenant Partitions Registry
        RowColumnValueStore<MiruTenantId, MiruHost, MiruPartitionId, MiruVoidByte, ? extends Exception> expectedTenantPartitionsRegistry =
            setOfSortedMapsImplInitializer.initialize(
                tableNameSpace,
                "miru.reg.th", // Tenant+Host
                "e",
                    new DefaultRowColumnValueStoreMarshaller<>(
                            new MiruTenantIdMarshaller(),
                            new SaltingDelegatingMarshaller<>(new MiruHostMarshaller()),
                            new MiruPartitionIdMarshaller(),
                            new MiruVoidByteMarshaller()),
                    new CurrentTimestamper()
            );

        // Miru Replica Registry
        RowColumnValueStore<MiruTenantId, MiruPartitionId, Long, MiruHost, ? extends Exception> replicaRegistry =
            setOfSortedMapsImplInitializer.initialize(
                tableNameSpace,
                "miru.reg.tp", // Tenant+Partition
                "r",
                new String[] { "b" },
                new DefaultRowColumnValueStoreMarshaller<>(
                        new MiruTenantIdMarshaller(),
                        new SaltingDelegatingMarshaller<>(new MiruPartitionIdMarshaller()),
                        new LongTypeMarshaller(),
                        new MiruHostMarshaller()),
                new CurrentTimestamper()
            );

        // Miru Topology Registry
        RowColumnValueStore<MiruVoidByte, MiruTenantId, MiruTopologyColumnKey, MiruTopologyColumnValue, ? extends Exception> topologyRegistry =
            setOfSortedMapsImplInitializer.initialize(
                tableNameSpace,
                "miru.reg.t", // Tenant
                "t",
                new String[] { "c", "p" },
                new DefaultRowColumnValueStoreMarshaller<>(
                        new MiruVoidByteMarshaller(),
                        new SaltingDelegatingMarshaller<>(new MiruTenantIdMarshaller()),
                        new MiruTopologyColumnKeyMarshaller(),
                        new MiruTopologyColumnValueMarshaller()),
                new CurrentTimestamper()
            );

        // Miru Config Registry
        RowColumnValueStore<MiruVoidByte, MiruTenantId, MiruTenantConfigFields, Long, ? extends Exception> configRegistry =
            setOfSortedMapsImplInitializer.initialize(
                tableNameSpace,
                "miru.reg.t", // Tenant
                "c",
                new String[] { "t", "p" },
                new DefaultRowColumnValueStoreMarshaller<>(
                        new MiruVoidByteMarshaller(),
                        new SaltingDelegatingMarshaller<>(new MiruTenantIdMarshaller()),
                        new MiruTenantConfigFieldsMarshaller(),
                        new LongTypeMarshaller()),
                new CurrentTimestamper()
            );

        // Miru Writer + PartitionId Registry
        RowColumnValueStore<MiruVoidByte, MiruTenantId, Integer, MiruPartitionId, ? extends Exception> writerPartitionRegistry =
            setOfSortedMapsImplInitializer.initialize(
                tableNameSpace,
                "miru.reg.t", // Tenant
                "p",
                new String[] { "t", "c" },
                new DefaultRowColumnValueStoreMarshaller<>(
                    new MiruVoidByteMarshaller(),
                        new SaltingDelegatingMarshaller<>(new MiruTenantIdMarshaller()),
                    new IntegerTypeMarshaller(),
                    new MiruPartitionIdMarshaller()),
                new CurrentTimestamper()
            );

        // Miru Activity Lookup Table
        RowColumnValueStore<MiruVoidByte, MiruTenantId, Long, MiruActivityLookupEntry, ? extends Exception> activityLookupTable =
            setOfSortedMapsImplInitializer.initialize(
                tableNameSpace,
                "miru.lookup.t", // Tenant
                "a",
                new DefaultRowColumnValueStoreMarshaller<>(
                    new MiruVoidByteMarshaller(),
                        new SaltingDelegatingMarshaller<>(new MiruTenantIdMarshaller()),
                    new LongTypeMarshaller(),
                    new MiruActivityLookupEntryMarshaller()),
                new CurrentTimestamper()
            );

        Injector injector = Guice.createInjector(new MiruRegistryInitializerModule(config,
            hostsRegistry, expectedTenantsRegistry, expectedTenantPartitionsRegistry, replicaRegistry, topologyRegistry, configRegistry,
            writerPartitionRegistry, activityLookupTable, clusterRegistryClass));
        return injector.getInstance(MiruRegistryInitializer.class);
    }

    private final MiruRegistryConfig config;

    private final RowColumnValueStore<MiruVoidByte, MiruHost, MiruHostsColumnKey, MiruHostsColumnValue, ? extends Exception> hostsRegistry;
    private final RowColumnValueStore<MiruVoidByte, MiruHost, MiruTenantId, MiruVoidByte, ? extends Exception> expectedTenantsRegistry;
    private final RowColumnValueStore<MiruTenantId, MiruHost, MiruPartitionId, MiruVoidByte, ? extends Exception> expectedTenantPartitionsRegistry;
    private final RowColumnValueStore<MiruTenantId, MiruPartitionId, Long, MiruHost, ? extends Exception> replicaRegistry;
    private final RowColumnValueStore<MiruVoidByte, MiruTenantId, MiruTopologyColumnKey, MiruTopologyColumnValue, ? extends Exception> topologyRegistry;
    private final RowColumnValueStore<MiruVoidByte, MiruTenantId, MiruTenantConfigFields, Long, ? extends Exception> configRegistry;
    private final RowColumnValueStore<MiruVoidByte, MiruTenantId, Integer, MiruPartitionId, ? extends Exception> writerPartitionRegistry;
    private final RowColumnValueStore<MiruVoidByte, MiruTenantId, Long, MiruActivityLookupEntry, ? extends Exception> activityLookupTable;

    private final MiruClusterRegistry clusterRegistry;

    @Inject
    public MiruRegistryInitializer(
        MiruRegistryConfig config,
        RowColumnValueStore<MiruVoidByte, MiruHost, MiruHostsColumnKey, MiruHostsColumnValue, ? extends Exception> hostsRegistry,
        RowColumnValueStore<MiruVoidByte, MiruHost, MiruTenantId, MiruVoidByte, ? extends Exception> expectedTenantsRegistry,
        RowColumnValueStore<MiruTenantId, MiruHost, MiruPartitionId, MiruVoidByte, ? extends Exception> expectedTenantPartitionsRegistry,
        RowColumnValueStore<MiruTenantId, MiruPartitionId, Long, MiruHost, ? extends Exception> replicaRegistry,
        RowColumnValueStore<MiruVoidByte, MiruTenantId, MiruTopologyColumnKey, MiruTopologyColumnValue, ? extends Exception> topologyRegistry,
        RowColumnValueStore<MiruVoidByte, MiruTenantId, MiruTenantConfigFields, Long, ? extends Exception> configRegistry,
        RowColumnValueStore<MiruVoidByte, MiruTenantId, Integer, MiruPartitionId, ? extends Exception> writerPartitionRegistry,
        RowColumnValueStore<MiruVoidByte, MiruTenantId, Long, MiruActivityLookupEntry, ? extends Exception> activityLookupTable,
        MiruClusterRegistry clusterRegistry) {
        this.config = config;
        this.hostsRegistry = hostsRegistry;
        this.expectedTenantsRegistry = expectedTenantsRegistry;
        this.expectedTenantPartitionsRegistry = expectedTenantPartitionsRegistry;
        this.replicaRegistry = replicaRegistry;
        this.topologyRegistry = topologyRegistry;
        this.configRegistry = configRegistry;
        this.writerPartitionRegistry = writerPartitionRegistry;
        this.activityLookupTable = activityLookupTable;
        this.clusterRegistry = clusterRegistry;
    }

    public MiruRegistryConfig getConfig() {
        return config;
    }

    public RowColumnValueStore<MiruVoidByte, MiruHost, MiruHostsColumnKey, MiruHostsColumnValue, ? extends Exception> getHostsRegistry() {
        return hostsRegistry;
    }

    public RowColumnValueStore<MiruVoidByte, MiruHost, MiruTenantId, MiruVoidByte, ? extends Exception> getExpectedTenantsRegistry() {
        return expectedTenantsRegistry;
    }

    public RowColumnValueStore<MiruTenantId, MiruHost, MiruPartitionId, MiruVoidByte, ? extends Exception> getExpectedTenantPartitionsRegistry() {
        return expectedTenantPartitionsRegistry;
    }

    public RowColumnValueStore<MiruTenantId, MiruPartitionId, Long, MiruHost, ? extends Exception> getReplicaRegistry() {
        return replicaRegistry;
    }

    public RowColumnValueStore<MiruVoidByte, MiruTenantId, MiruTopologyColumnKey, MiruTopologyColumnValue, ? extends Exception> getTopologyRegistry() {
        return topologyRegistry;
    }

    public RowColumnValueStore<MiruVoidByte, MiruTenantId, MiruTenantConfigFields, Long, ? extends Exception> getConfigRegistry() {
        return configRegistry;
    }

    public RowColumnValueStore<MiruVoidByte, MiruTenantId, Integer, MiruPartitionId, ? extends Exception> getWriterPartitionRegistry() {
        return writerPartitionRegistry;
    }

    public RowColumnValueStore<MiruVoidByte, MiruTenantId, Long, MiruActivityLookupEntry, ? extends Exception> getActivityLookupTable() {
        return activityLookupTable;
    }

    public MiruClusterRegistry getClusterRegistry() {
        return clusterRegistry;
    }
}
