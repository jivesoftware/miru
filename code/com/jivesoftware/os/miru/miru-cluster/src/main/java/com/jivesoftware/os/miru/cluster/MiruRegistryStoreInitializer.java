package com.jivesoftware.os.miru.cluster;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.jivesoftware.os.miru.api.MiruHost;
import com.jivesoftware.os.miru.api.activity.MiruPartitionId;
import com.jivesoftware.os.miru.api.activity.schema.MiruSchema;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.api.marshall.JacksonJsonObjectTypeMarshaller;
import com.jivesoftware.os.miru.api.marshall.MiruPartitionIdMarshaller;
import com.jivesoftware.os.miru.api.marshall.MiruTenantIdMarshaller;
import com.jivesoftware.os.miru.api.marshall.MiruVoidByte;
import com.jivesoftware.os.miru.api.marshall.MiruVoidByteMarshaller;
import com.jivesoftware.os.miru.cluster.marshaller.MiruHostMarshaller;
import com.jivesoftware.os.miru.cluster.marshaller.MiruHostsColumnKeyMarshaller;
import com.jivesoftware.os.miru.cluster.marshaller.MiruHostsColumnValueMarshaller;
import com.jivesoftware.os.miru.cluster.marshaller.MiruSchemaColumnKeyMarshaller;
import com.jivesoftware.os.miru.cluster.marshaller.MiruTenantConfigFieldsMarshaller;
import com.jivesoftware.os.miru.cluster.marshaller.MiruTopologyColumnKeyMarshaller;
import com.jivesoftware.os.miru.cluster.marshaller.MiruTopologyColumnValueMarshaller;
import com.jivesoftware.os.miru.cluster.rcvs.MiruHostsColumnKey;
import com.jivesoftware.os.miru.cluster.rcvs.MiruHostsColumnValue;
import com.jivesoftware.os.miru.cluster.rcvs.MiruSchemaColumnKey;
import com.jivesoftware.os.miru.cluster.rcvs.MiruTopologyColumnKey;
import com.jivesoftware.os.miru.cluster.rcvs.MiruTopologyColumnValue;
import com.jivesoftware.os.rcvs.api.DefaultRowColumnValueStoreMarshaller;
import com.jivesoftware.os.rcvs.api.RowColumnValueStore;
import com.jivesoftware.os.rcvs.api.RowColumnValueStoreInitializer;
import com.jivesoftware.os.rcvs.api.timestamper.CurrentTimestamper;
import com.jivesoftware.os.rcvs.marshall.id.SaltingDelegatingMarshaller;
import com.jivesoftware.os.rcvs.marshall.primatives.ByteArrayTypeMarshaller;
import com.jivesoftware.os.rcvs.marshall.primatives.LongTypeMarshaller;

/**
 * ------
 * miru.reg.h [Void, Host] [h=hosts, e=expectedTenants]
 * ---
 * - h: Heartbeat = void (timestamp for recency)
 * ---
 * - e: Tenant1 = void
 * - e: Tenant2 = void
 * - e: Tenant3 = void
 * ------
 * miru.reg.th [Tenant, Host] [e=expectedTenantPartitions]
 * ---
 * - e: Partition1 = void
 * - e: Partition2 = void
 * - e: Partition3 = void
 * ------
 * miru.reg.tp [Tenant, Partition] [r=replica]
 * ---
 * TODO could be r: Host1 = 1 (timestamp for recency)
 * - r: InverseDateAdded1 = Host1
 * - r: InverseDateAdded2 = Host2
 * - r: InverseDateAdded3 = Host3
 * ------
 * miru.reg.t [Void, Tenant] [t=topology, c=config, p=partition, s=schema]
 * ---
 * - t: Partition1 Host1 = State BackingStorage (timestamp for staleness)
 * - t: Partition1 Host1 = State BackingStorage (timestamp for staleness)
 * - t: Partition1 Host2 = State BackingStorage (timestamp for staleness)
 * - t: Partition1 Host3 = State BackingStorage (timestamp for staleness)
 * - t: Partition2 Host1 = State BackingStorage (timestamp for staleness)
 * - t: Partition2 Host2 = State BackingStorage (timestamp for staleness)
 * - t: Partition2 Host3 = State BackingStorage (timestamp for staleness)
 * ---
 * - c: NumberOfReplicas = 3
 * - c: TopologyIsStaleAfterMillis = 1_000_000
 * ---
 * - p: WriterId = versioned PartitionId (partitionIdX, partitionIdX-1, ...)
 * ---
 * - s: Schema = SchemaJSON (timestamp for version)
 * ------
 */
public class MiruRegistryStoreInitializer {

    public MiruRegistryStore initialize(String tableNameSpace,
        RowColumnValueStoreInitializer<? extends Exception> rowColumnValueStoreInitializer,
        ObjectMapper objectMapper) throws Exception {

        // Miru Hosts Registry
        RowColumnValueStore<MiruVoidByte, MiruHost, MiruHostsColumnKey, MiruHostsColumnValue, ? extends Exception> hostsRegistry =
            rowColumnValueStoreInitializer.initialize(
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
            rowColumnValueStoreInitializer.initialize(
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
            rowColumnValueStoreInitializer.initialize(
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
        RowColumnValueStore<MiruTenantId, MiruPartitionId, Long, MiruHost, ? extends Exception> replicaRegistry = rowColumnValueStoreInitializer.initialize(
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
            rowColumnValueStoreInitializer.initialize(
                tableNameSpace,
                "miru.reg.t", // Tenant
                "t",
                new String[] { "c", "p", "s" },
                new DefaultRowColumnValueStoreMarshaller<>(
                    new MiruVoidByteMarshaller(),
                    new SaltingDelegatingMarshaller<>(new MiruTenantIdMarshaller()),
                    new MiruTopologyColumnKeyMarshaller(),
                    new MiruTopologyColumnValueMarshaller()),
                new CurrentTimestamper()
            );

        // Miru Config Registry
        RowColumnValueStore<MiruVoidByte, MiruTenantId, MiruTenantConfigFields, Long, ? extends Exception> configRegistry =
            rowColumnValueStoreInitializer.initialize(
                tableNameSpace,
                "miru.reg.t", // Tenant
                "c",
                new String[] { "t", "p", "s" },
                new DefaultRowColumnValueStoreMarshaller<>(
                    new MiruVoidByteMarshaller(),
                    new SaltingDelegatingMarshaller<>(new MiruTenantIdMarshaller()),
                    new MiruTenantConfigFieldsMarshaller(),
                    new LongTypeMarshaller()),
                new CurrentTimestamper()
            );


        // Miru Schema Registry
        RowColumnValueStore<MiruVoidByte, MiruTenantId, MiruSchemaColumnKey, MiruSchema, ? extends Exception> schemaRegistry =
            rowColumnValueStoreInitializer.initialize(
                tableNameSpace,
                "miru.reg.t", // Tenant
                "s",
                new String[] { "t", "c", "p" },
                new DefaultRowColumnValueStoreMarshaller<>(
                    new MiruVoidByteMarshaller(),
                    new SaltingDelegatingMarshaller<>(new MiruTenantIdMarshaller()),
                    new MiruSchemaColumnKeyMarshaller(),
                    new JacksonJsonObjectTypeMarshaller<>(MiruSchema.class, objectMapper)),
                new CurrentTimestamper()
            );


        // Miru Activity Payload Table
        RowColumnValueStore<MiruVoidByte, MiruTenantId, Long, byte[], ? extends Exception> activityPayloadTable =
            rowColumnValueStoreInitializer.initialize(
                tableNameSpace,
                "miru.payload.t", // Tenant
                "a",
                new DefaultRowColumnValueStoreMarshaller<>(
                    new MiruVoidByteMarshaller(),
                    new SaltingDelegatingMarshaller<>(new MiruTenantIdMarshaller()),
                    new LongTypeMarshaller(),
                    new ByteArrayTypeMarshaller()),
                new CurrentTimestamper()
            );

        return new MiruRegistryStore(hostsRegistry,
            expectedTenantsRegistry,
            expectedTenantPartitionsRegistry,
            replicaRegistry,
            topologyRegistry,
            configRegistry,
            schemaRegistry,
            activityPayloadTable);
    }

}
