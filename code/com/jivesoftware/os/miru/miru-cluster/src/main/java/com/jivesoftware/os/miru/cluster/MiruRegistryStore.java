/*
 * Copyright 2014 Jivesoftware.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.jivesoftware.os.miru.cluster;

import com.jivesoftware.os.miru.api.MiruHost;
import com.jivesoftware.os.miru.api.activity.MiruPartitionId;
import com.jivesoftware.os.miru.api.activity.schema.MiruSchema;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.api.marshall.MiruVoidByte;
import com.jivesoftware.os.miru.cluster.rcvs.MiruHostsColumnKey;
import com.jivesoftware.os.miru.cluster.rcvs.MiruHostsColumnValue;
import com.jivesoftware.os.miru.cluster.rcvs.MiruSchemaColumnKey;
import com.jivesoftware.os.miru.cluster.rcvs.MiruTopologyColumnKey;
import com.jivesoftware.os.miru.cluster.rcvs.MiruTopologyColumnValue;
import com.jivesoftware.os.rcvs.api.RowColumnValueStore;

/**
 * @author jonathan
 */
public class MiruRegistryStore {

    private final RowColumnValueStore<MiruVoidByte, MiruHost, MiruHostsColumnKey, MiruHostsColumnValue, ? extends Exception> hostsRegistry;
    private final RowColumnValueStore<MiruVoidByte, MiruHost, MiruTenantId, MiruVoidByte, ? extends Exception> expectedTenantsRegistry;
    private final RowColumnValueStore<MiruTenantId, MiruHost, MiruPartitionId, MiruVoidByte, ? extends Exception> expectedTenantPartitionsRegistry;
    private final RowColumnValueStore<MiruTenantId, MiruPartitionId, Long, MiruHost, ? extends Exception> replicaRegistry;
    private final RowColumnValueStore<MiruVoidByte, MiruTenantId, MiruTopologyColumnKey, MiruTopologyColumnValue, ? extends Exception> topologyRegistry;
    private final RowColumnValueStore<MiruVoidByte, MiruTenantId, MiruTenantConfigFields, Long, ? extends Exception> configRegistry;
    private final RowColumnValueStore<MiruVoidByte, MiruTenantId, MiruSchemaColumnKey, MiruSchema, ? extends Exception> schemaRegistry;
    private final RowColumnValueStore<MiruVoidByte, MiruTenantId, Long, byte[], ? extends Exception> activityPayloadTable;

    public MiruRegistryStore(RowColumnValueStore<MiruVoidByte, MiruHost, MiruHostsColumnKey, MiruHostsColumnValue, ? extends Exception> hostsRegistry,
        RowColumnValueStore<MiruVoidByte, MiruHost, MiruTenantId, MiruVoidByte, ? extends Exception> expectedTenantsRegistry,
        RowColumnValueStore<MiruTenantId, MiruHost, MiruPartitionId, MiruVoidByte, ? extends Exception> expectedTenantPartitionsRegistry,
        RowColumnValueStore<MiruTenantId, MiruPartitionId, Long, MiruHost, ? extends Exception> replicaRegistry,
        RowColumnValueStore<MiruVoidByte, MiruTenantId, MiruTopologyColumnKey, MiruTopologyColumnValue, ? extends Exception> topologyRegistry,
        RowColumnValueStore<MiruVoidByte, MiruTenantId, MiruTenantConfigFields, Long, ? extends Exception> configRegistry,
        RowColumnValueStore<MiruVoidByte, MiruTenantId, MiruSchemaColumnKey, MiruSchema, ? extends Exception> schemaRegistry,
        RowColumnValueStore<MiruVoidByte, MiruTenantId, Long, byte[], ? extends Exception> activityPayloadTable) {
        this.hostsRegistry = hostsRegistry;
        this.expectedTenantsRegistry = expectedTenantsRegistry;
        this.expectedTenantPartitionsRegistry = expectedTenantPartitionsRegistry;
        this.replicaRegistry = replicaRegistry;
        this.topologyRegistry = topologyRegistry;
        this.configRegistry = configRegistry;
        this.schemaRegistry = schemaRegistry;
        this.activityPayloadTable = activityPayloadTable;
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

    public RowColumnValueStore<MiruVoidByte, MiruTenantId, MiruSchemaColumnKey, MiruSchema, ? extends Exception> getSchemaRegistry() {
        return schemaRegistry;
    }

    public RowColumnValueStore<MiruVoidByte, MiruTenantId, Long, byte[], ? extends Exception> getActivityPayloadTable() {
        return activityPayloadTable;
    }
}
