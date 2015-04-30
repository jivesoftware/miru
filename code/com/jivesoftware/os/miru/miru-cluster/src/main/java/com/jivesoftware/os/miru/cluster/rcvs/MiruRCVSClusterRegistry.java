package com.jivesoftware.os.miru.cluster.rcvs;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimaps;
import com.google.common.collect.SetMultimap;
import com.google.common.collect.Sets;
import com.google.common.collect.Table;
import com.jivesoftware.os.jive.utils.base.interfaces.CallbackStream;
import com.jivesoftware.os.jive.utils.base.util.locks.StripingLocksProvider;
import com.jivesoftware.os.miru.api.MiruBackingStorage;
import com.jivesoftware.os.miru.api.MiruHost;
import com.jivesoftware.os.miru.api.MiruPartition;
import com.jivesoftware.os.miru.api.MiruPartitionCoord;
import com.jivesoftware.os.miru.api.MiruPartitionCoordInfo;
import com.jivesoftware.os.miru.api.MiruPartitionState;
import com.jivesoftware.os.miru.api.MiruTopologyStatus;
import com.jivesoftware.os.miru.api.activity.MiruPartitionId;
import com.jivesoftware.os.miru.api.activity.schema.MiruSchema;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.api.marshall.MiruVoidByte;
import com.jivesoftware.os.miru.api.topology.HostHeartbeat;
import com.jivesoftware.os.miru.api.topology.MiruPartitionActive;
import com.jivesoftware.os.miru.api.topology.MiruPartitionActiveUpdate;
import com.jivesoftware.os.miru.api.topology.MiruTenantConfig;
import com.jivesoftware.os.miru.api.topology.MiruTenantTopologyUpdate;
import com.jivesoftware.os.miru.api.topology.NamedCursor;
import com.jivesoftware.os.miru.api.topology.NamedCursorsResult;
import com.jivesoftware.os.miru.cluster.MiruClusterRegistry;
import com.jivesoftware.os.miru.cluster.MiruReplicaSet;
import com.jivesoftware.os.miru.cluster.MiruTenantConfigFields;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import com.jivesoftware.os.rcvs.api.ColumnValueAndTimestamp;
import com.jivesoftware.os.rcvs.api.KeyedColumnValueCallbackStream;
import com.jivesoftware.os.rcvs.api.RowColumValueTimestampAdd;
import com.jivesoftware.os.rcvs.api.RowColumnValueStore;
import com.jivesoftware.os.rcvs.api.TenantIdAndRow;
import com.jivesoftware.os.rcvs.api.timestamper.Timestamper;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.commons.lang.mutable.MutableLong;

public class MiruRCVSClusterRegistry implements MiruClusterRegistry {

    private static final MetricLogger log = MetricLoggerFactory.getLogger();

    // See MiruRegistryInitializer for schema information
    private final Timestamper timestamper;
    private final RowColumnValueStore<MiruVoidByte, MiruHost, MiruHostsColumnKey, MiruHostsColumnValue, ? extends Exception> hostsRegistry;
    private final RowColumnValueStore<MiruVoidByte, MiruHost, MiruTenantId, MiruVoidByte, ? extends Exception> expectedTenantsRegistry;
    private final RowColumnValueStore<MiruVoidByte, MiruHost, MiruTenantId, MiruVoidByte, ? extends Exception> topologyUpdatesRegistry;
    private final RowColumnValueStore<MiruTenantId, MiruHost, MiruPartitionId, MiruVoidByte, ? extends Exception> expectedTenantPartitionsRegistry;
    private final RowColumnValueStore<MiruTenantId, MiruPartitionId, Long, MiruHost, ? extends Exception> replicaRegistry;
    private final RowColumnValueStore<MiruVoidByte, MiruTenantId, MiruTopologyColumnKey, MiruTopologyColumnValue, ? extends Exception> topologyRegistry;
    private final RowColumnValueStore<MiruVoidByte, MiruTenantId, MiruTenantConfigFields, Long, ? extends Exception> configRegistry;
    private final RowColumnValueStore<MiruVoidByte, MiruTenantId, MiruSchemaColumnKey, MiruSchema, ? extends Exception> schemaRegistry;

    private final int defaultNumberOfReplicas;
    private final long defaultTopologyIsStaleAfterMillis;
    private final long defaultTopologyIsIdleAfterMillis;

    private final StripingLocksProvider<TenantPartitionHostKey> topologyLocks = new StripingLocksProvider<>(64);

    public MiruRCVSClusterRegistry(Timestamper timestamper,
        RowColumnValueStore<MiruVoidByte, MiruHost, MiruHostsColumnKey, MiruHostsColumnValue, ? extends Exception> hostsRegistry,
        RowColumnValueStore<MiruVoidByte, MiruHost, MiruTenantId, MiruVoidByte, ? extends Exception> expectedTenantsRegistry,
        RowColumnValueStore<MiruVoidByte, MiruHost, MiruTenantId, MiruVoidByte, ? extends Exception> topologyUpdatesRegistry,
        RowColumnValueStore<MiruTenantId, MiruHost, MiruPartitionId, MiruVoidByte, ? extends Exception> expectedTenantPartitionsRegistry,
        RowColumnValueStore<MiruTenantId, MiruPartitionId, Long, MiruHost, ? extends Exception> replicaRegistry,
        RowColumnValueStore<MiruVoidByte, MiruTenantId, MiruTopologyColumnKey, MiruTopologyColumnValue, ? extends Exception> topologyRegistry,
        RowColumnValueStore<MiruVoidByte, MiruTenantId, MiruTenantConfigFields, Long, ? extends Exception> configRegistry,
        RowColumnValueStore<MiruVoidByte, MiruTenantId, MiruSchemaColumnKey, MiruSchema, ? extends Exception> schemaRegistry,
        int defaultNumberOfReplicas,
        long defaultTopologyIsStaleAfterMillis,
        long defaultTopologyIsIdleAfterMillis) throws Exception {

        this.timestamper = timestamper;
        this.hostsRegistry = hostsRegistry;
        this.expectedTenantsRegistry = expectedTenantsRegistry;
        this.topologyUpdatesRegistry = topologyUpdatesRegistry;
        this.expectedTenantPartitionsRegistry = expectedTenantPartitionsRegistry;
        this.replicaRegistry = replicaRegistry;
        this.topologyRegistry = topologyRegistry;
        this.configRegistry = configRegistry;
        this.schemaRegistry = schemaRegistry;
        this.defaultNumberOfReplicas = defaultNumberOfReplicas;
        this.defaultTopologyIsStaleAfterMillis = defaultTopologyIsStaleAfterMillis;
        this.defaultTopologyIsIdleAfterMillis = defaultTopologyIsIdleAfterMillis;
    }

    @Override
    public void sendHeartbeatForHost(MiruHost miruHost) throws Exception {
        hostsRegistry.add(MiruVoidByte.INSTANCE, miruHost, MiruHostsColumnKey.heartbeat, new MiruHostsColumnValue(), null, timestamper);
    }

    @Override
    public LinkedHashSet<HostHeartbeat> getAllHosts() throws Exception {
        final LinkedHashSet<HostHeartbeat> hostHeartbeats = Sets.newLinkedHashSet();
        final List<KeyedColumnValueCallbackStream<MiruHost, MiruHostsColumnKey, MiruHostsColumnValue, Long>> streams = Lists.newArrayList();

        hostsRegistry.getAllRowKeys(1_000, null, new CallbackStream<TenantIdAndRow<MiruVoidByte, MiruHost>>() {
            @Override
            public TenantIdAndRow<MiruVoidByte, MiruHost> callback(TenantIdAndRow<MiruVoidByte, MiruHost> value) throws Exception {
                if (value != null) {
                    final MiruHost host = value.getRow();
                    streams.add(new KeyedColumnValueCallbackStream<>(
                        host,
                        new CallbackStream<ColumnValueAndTimestamp<MiruHostsColumnKey, MiruHostsColumnValue, Long>>() {

                            @Override
                            public ColumnValueAndTimestamp<MiruHostsColumnKey, MiruHostsColumnValue, Long> callback(
                                ColumnValueAndTimestamp<MiruHostsColumnKey, MiruHostsColumnValue, Long> columnValueAndTimestamp) throws Exception {

                                    if (columnValueAndTimestamp != null
                                    && columnValueAndTimestamp.getColumn().getIndex() == MiruHostsColumnKey.heartbeat.getIndex()) {
                                        MiruHostsColumnValue value = columnValueAndTimestamp.getValue();
                                        hostHeartbeats.add(new HostHeartbeat(host, columnValueAndTimestamp.getTimestamp()));
                                    }
                                    return columnValueAndTimestamp;
                                }
                        }));
                }
                return value;
            }
        });

        hostsRegistry.multiRowGetAll(MiruVoidByte.INSTANCE, streams);

        return hostHeartbeats;
    }

    @Override
    public List<MiruTenantId> getTenantsForHost(MiruHost miruHost) throws Exception {
        final List<MiruTenantId> tenants = Lists.newArrayList();
        expectedTenantsRegistry.getEntrys(MiruVoidByte.INSTANCE, miruHost, null, Long.MAX_VALUE, 1_000, false, null, null,
            new CallbackStream<ColumnValueAndTimestamp<MiruTenantId, MiruVoidByte, Long>>() {
                @Override
                public ColumnValueAndTimestamp<MiruTenantId, MiruVoidByte, Long> callback(
                    ColumnValueAndTimestamp<MiruTenantId, MiruVoidByte, Long> value) throws Exception {
                    if (value != null) {
                        tenants.add(value.getColumn());
                    }
                    return value;
                }
            });
        return tenants;
    }

    @Override
    public MiruTenantConfig getTenantConfig(MiruTenantId tenantId) throws Exception {
        ColumnValueAndTimestamp<MiruTenantConfigFields, Long, Long>[] valueAndTimestamps = configRegistry
            .multiGetEntries(MiruVoidByte.INSTANCE, tenantId, MiruTenantConfigFields.values(), null, null);

        Map<String, Long> config = Maps.newHashMap();
        if (valueAndTimestamps != null) {
            for (ColumnValueAndTimestamp<MiruTenantConfigFields, Long, Long> valueAndTimestamp : valueAndTimestamps) {
                if (valueAndTimestamp != null) {
                    config.put(valueAndTimestamp.getColumn().name(), valueAndTimestamp.getValue());
                }
            }
        }

        return new MiruTenantConfig(config);
    }

    @Override
    public int getNumberOfReplicas(MiruTenantId tenantId) throws Exception {
        return getTenantConfig(tenantId).getInt(MiruTenantConfigFields.number_of_replicas.name(), defaultNumberOfReplicas);
    }

    @Override
    public List<MiruPartition> getPartitionsForTenant(MiruTenantId tenantId) throws Exception {
        final MiruTenantConfig config = getTenantConfig(tenantId);
        return Lists.transform(
            getTopologyStatusByState(config, tenantId, Optional.<MiruHost>absent()),
            topologyStatusToPartition);
    }

    @Override
    public List<MiruPartition> getPartitionsForTenantHost(MiruTenantId tenantId, MiruHost host) throws Exception {
        final MiruTenantConfig config = getTenantConfig(tenantId);
        return Lists.transform(
            getTopologyStatusByState(config, tenantId, Optional.of(host)),
            topologyStatusToPartition);
    }

    @Override
    public List<MiruTopologyStatus> getTopologyStatusForTenant(MiruTenantId tenantId) throws Exception {
        final MiruTenantConfig config = getTenantConfig(tenantId);
        return getTopologyStatusByState(config, tenantId, Optional.<MiruHost>absent());
    }

    @Override
    public List<MiruTopologyStatus> getTopologyStatusForTenantHost(MiruTenantId tenantId, MiruHost host) throws Exception {
        final MiruTenantConfig config = getTenantConfig(tenantId);
        return getTopologyStatusByState(config, tenantId, Optional.of(host));
    }

    @Override
    public Map<MiruPartitionId, MiruReplicaSet> getReplicaSets(MiruTenantId tenantId, Collection<MiruPartitionId> requiredPartitionId) throws Exception {

        MiruTenantConfig config = getTenantConfig(tenantId);
        int numberOfReplicas = config.getInt(MiruTenantConfigFields.number_of_replicas.name(), defaultNumberOfReplicas);
        SetMultimap<MiruPartitionId, MiruHost> perPartitonHostsWithReplica = getHostsWithReplica(tenantId, requiredPartitionId, numberOfReplicas);

        Map<MiruPartitionId, List<MiruTopologyStatus>> topologyStatusByState = getPartitionTopologyStatusByState(config,
            tenantId,
            requiredPartitionId,
            perPartitonHostsWithReplica);

        Map<MiruPartitionId, MiruReplicaSet> replicaSets = new HashMap<>();
        for (MiruPartitionId miruPartitionId : requiredPartitionId) {

            Set<MiruHost> partitionHosts = perPartitonHostsWithReplica.get(miruPartitionId);
            int countOfMissingReplicas = numberOfReplicas - partitionHosts.size();

            List<MiruPartition> partitions = Lists.transform(
                topologyStatusByState.get(miruPartitionId),
                topologyStatusToPartition);

            replicaSets.put(miruPartitionId, new MiruReplicaSet(extractPartitionsByState(partitions), partitionHosts, countOfMissingReplicas));
        }

        return replicaSets;
    }

    private ListMultimap<MiruPartitionState, MiruPartition> extractPartitionsByState(List<MiruPartition> partitions) {
        return Multimaps.index(partitions, new Function<MiruPartition, MiruPartitionState>() {
            @Override
            public MiruPartitionState apply(MiruPartition input) {
                return input.info.state;
            }
        });
    }

    private SetMultimap<MiruPartitionId, MiruHost> getHostsWithReplica(MiruTenantId tenantId,
        Collection<MiruPartitionId> requiredPartitionIds,
        final int numberOfReplicas) throws Exception {

        final SetMultimap<MiruPartitionId, MiruHost> hostsWithReplicaPerPartition = HashMultimap.create();
        final Set<MiruHost> availableHosts = Sets.newHashSet();
        for (HostHeartbeat heartbeat : getAllHosts()) {
            availableHosts.add(heartbeat.host);
        }
        List<KeyedColumnValueCallbackStream<MiruPartitionId, Long, MiruHost, Long>> rowKeyCallbackStreamPair = new ArrayList<>();
        for (final MiruPartitionId miruPartitionId : requiredPartitionIds) {
            rowKeyCallbackStreamPair.add(new KeyedColumnValueCallbackStream<>(miruPartitionId,
                new CallbackStream<ColumnValueAndTimestamp<Long, MiruHost, Long>>() {

                    @Override
                    public ColumnValueAndTimestamp<Long, MiruHost, Long> callback(ColumnValueAndTimestamp<Long, MiruHost, Long> v) throws Exception {
                        if (v != null) {
                            MiruHost host = v.getValue();
                            if (availableHosts.contains(host)) {
                                hostsWithReplicaPerPartition.put(miruPartitionId, host);
                                if (hostsWithReplicaPerPartition.get(miruPartitionId).size() < numberOfReplicas) {
                                    // keep going until we find as many replicas as we need
                                    return v;
                                }
                            }
                        }
                        return null;
                    }
                }));
        }
        replicaRegistry.multiRowGetAll(tenantId, rowKeyCallbackStreamPair);
        return hostsWithReplicaPerPartition;
    }

    @Override
    public void updateTopologies(MiruHost host, Collection<TopologyUpdate> topologyUpdates) throws Exception {
        //TODO batch update more efficiently
        for (TopologyUpdate topologyUpdate : topologyUpdates) {
            MiruPartitionCoord coord = topologyUpdate.coord;
            Optional<MiruPartitionCoordInfo> optionalInfo = topologyUpdate.optionalInfo;
            Optional<Long> refreshTimestamp = topologyUpdate.refreshTimestamp;

            synchronized (topologyLocks.lock(new TenantPartitionHostKey(coord))) {
                MiruPartitionCoordInfo coordInfo;
                if (optionalInfo.isPresent()) {
                    coordInfo = optionalInfo.get();
                } else {
                    ColumnValueAndTimestamp<MiruTopologyColumnKey, MiruTopologyColumnValue, Long> valueAndTimestamp = getTopologyValueAndTimestamp(
                        coord.tenantId, coord.partitionId, coord.host);
                    if (valueAndTimestamp != null) {
                        coordInfo = new MiruPartitionCoordInfo(valueAndTimestamp.getValue().state, valueAndTimestamp.getValue().storage);
                    } else {
                        coordInfo = new MiruPartitionCoordInfo(MiruPartitionState.offline, MiruBackingStorage.memory);
                    }
                }

                long timestamp;
                if (refreshTimestamp.isPresent()) {
                    timestamp = refreshTimestamp.get();
                } else {
                    ColumnValueAndTimestamp<MiruTopologyColumnKey, MiruTopologyColumnValue, Long> valueAndTimestamp = getTopologyValueAndTimestamp(
                        coord.tenantId, coord.partitionId, coord.host);
                    if (valueAndTimestamp != null) {
                        timestamp = valueAndTimestamp.getValue().lastActiveTimestamp;
                    } else {
                        timestamp = 0;
                    }
                }

                MiruTopologyColumnValue update = new MiruTopologyColumnValue(coordInfo.state, coordInfo.storage, timestamp);

                topologyRegistry.add(MiruVoidByte.INSTANCE, coord.tenantId, new MiruTopologyColumnKey(coord.partitionId, coord.host),
                    update, null, timestamper);
                log.debug("Updated {} to {} at {}", new Object[]{coord, coordInfo, refreshTimestamp});
            }

            if (optionalInfo.isPresent()) {
                markTenantTopologyUpdated(Arrays.asList(coord.tenantId));
            }
        }
    }

    private void markTenantTopologyUpdated(List<MiruTenantId> tenantIds) throws Exception {
        List<RowColumValueTimestampAdd<MiruHost, MiruTenantId, MiruVoidByte>> adds = Lists.newArrayList();
        for (HostHeartbeat heartbeat : getAllHosts()) {
            for (MiruTenantId tenantId : tenantIds) {
                adds.add(new RowColumValueTimestampAdd<>(heartbeat.host, tenantId, MiruVoidByte.INSTANCE, null));
            }
        }
        topologyUpdatesRegistry.multiRowsMultiAdd(MiruVoidByte.INSTANCE, adds);
    }

    @Override
    public NamedCursorsResult<Collection<MiruTenantTopologyUpdate>> getTopologyUpdatesForHost(MiruHost host,
        Collection<NamedCursor> sinceCursors)
        throws Exception {

        long cursorTimestamp = 0;
        for (NamedCursor sinceCursor : sinceCursors) {
            if ("rcvs".equals(sinceCursor.name)) {
                cursorTimestamp = sinceCursor.id;
                break;
            }
        }
        final long acceptableTimestamp = cursorTimestamp - TimeUnit.MINUTES.toMillis(1); //TODO max GC pause or clock drift

        final List<MiruTenantTopologyUpdate> updates = Lists.newArrayList();
        final MutableLong highestTimestamp = new MutableLong(acceptableTimestamp);
        topologyUpdatesRegistry.getEntrys(MiruVoidByte.INSTANCE, host, null, Long.MAX_VALUE, 10_000, false, null, null,
            new CallbackStream<ColumnValueAndTimestamp<MiruTenantId, MiruVoidByte, Long>>() {
                @Override
                public ColumnValueAndTimestamp<MiruTenantId, MiruVoidByte, Long> callback(
                    ColumnValueAndTimestamp<MiruTenantId, MiruVoidByte, Long> cvat) throws Exception {
                    if (cvat != null) {
                        long timestamp = cvat.getTimestamp();
                        if (timestamp > acceptableTimestamp) {
                            updates.add(new MiruTenantTopologyUpdate(cvat.getColumn(), timestamp));
                            if (timestamp > highestTimestamp.longValue()) {
                                highestTimestamp.setValue(timestamp);
                            }
                        }
                    }
                    return cvat;
                }
            });
        return new NamedCursorsResult<Collection<MiruTenantTopologyUpdate>>(
            Collections.singletonList(new NamedCursor("rcvs", highestTimestamp.longValue())),
            updates);
    }

    @Override
    public NamedCursorsResult<Collection<MiruPartitionActiveUpdate>> getPartitionActiveUpdatesForHost(MiruHost host,
        Collection<NamedCursor> sinceCursors) throws Exception {

        List<MiruPartitionActiveUpdate> updates = Lists.newArrayList();
        for (MiruTenantId tenantId : getTenantsForHost(host)) {
            SetMultimap<MiruPartitionId, MiruHost> replicaHosts = HashMultimap.create();
            for (MiruTopologyStatus status : getTopologyStatusForTenant(tenantId)) {
                replicaHosts.put(status.partition.coord.partitionId, status.partition.coord.host);
                if (host.equals(status.partition.coord.host)) {
                    MiruPartitionActive partitionActive = isPartitionActive(status.partition.coord);
                    MiruPartitionId partitionId = status.partition.coord.partitionId;
                    updates.add(new MiruPartitionActiveUpdate(tenantId, partitionId.getId(), true, partitionActive.active, partitionActive.idle));
                }
            }
            for (Map.Entry<MiruPartitionId, Collection<MiruHost>> entry : replicaHosts.asMap().entrySet()) {
                if (!entry.getValue().contains(host)) {
                    updates.add(new MiruPartitionActiveUpdate(tenantId, entry.getKey().getId(), false, false, false));
                }
            }
        }
        return new NamedCursorsResult<Collection<MiruPartitionActiveUpdate>>(Collections.<NamedCursor>emptyList(), updates);
    }

    @Override
    public MiruPartitionActive isPartitionActive(MiruPartitionCoord coord) throws Exception {
        MiruTenantConfig config = getTenantConfig(coord.tenantId);
        return isTenantPartitionActiveForHost(coord.tenantId, coord.partitionId, coord.host, config);
    }

    @Override
    public void removeHost(final MiruHost host) throws Exception {
        final List<MiruTenantId> tenantIds = Lists.newArrayList();
        expectedTenantsRegistry.getEntrys(MiruVoidByte.INSTANCE, host, null, null, 1_000, false, null, null,
            new CallbackStream<ColumnValueAndTimestamp<MiruTenantId, MiruVoidByte, Long>>() {
                @Override
                public ColumnValueAndTimestamp<MiruTenantId, MiruVoidByte, Long> callback(ColumnValueAndTimestamp<MiruTenantId, MiruVoidByte, Long> v)
                throws Exception {
                    if (v != null) {
                        tenantIds.add(v.getColumn());
                    }
                    return v;
                }
            });

        for (MiruTenantId tenantId : tenantIds) {
            expectedTenantPartitionsRegistry.removeRow(tenantId, host, timestamper);
        }
        expectedTenantsRegistry.removeRow(MiruVoidByte.INSTANCE, host, timestamper);
        hostsRegistry.removeRow(MiruVoidByte.INSTANCE, host, timestamper);

        markTenantTopologyUpdated(tenantIds);
    }

    @Override
    public void removeTopology(MiruTenantId tenantId, MiruPartitionId partitionId, MiruHost host) throws Exception {
        topologyRegistry.remove(MiruVoidByte.INSTANCE, tenantId, new MiruTopologyColumnKey(partitionId, host), timestamper);
        markTenantTopologyUpdated(Arrays.asList(tenantId));
    }

    @Override
    public void topologiesForTenants(List<MiruTenantId> tenantIds, final CallbackStream<MiruTopologyStatus> callbackStream) throws Exception {
        final List<KeyedColumnValueCallbackStream<MiruTenantId, MiruTopologyColumnKey, MiruTopologyColumnValue, Long>> callbacks =
            Lists.newArrayListWithCapacity(tenantIds.size());
        final Table<MiruTenantId, MiruPartitionId, List<MiruTopologyStatus>> topologies = HashBasedTable.create();
        for (final MiruTenantId tenantId : tenantIds) {
            callbacks.add(new KeyedColumnValueCallbackStream<>(tenantId,
                new CallbackStream<ColumnValueAndTimestamp<MiruTopologyColumnKey, MiruTopologyColumnValue, Long>>() {
                    @Override
                    public ColumnValueAndTimestamp<MiruTopologyColumnKey, MiruTopologyColumnValue, Long> callback(
                        ColumnValueAndTimestamp<MiruTopologyColumnKey, MiruTopologyColumnValue, Long> c) throws Exception {
                            if (c != null) {
                                MiruTopologyColumnValue value = c.getValue();
                                MiruHost host = c.getColumn().host;
                                MiruTopologyStatus status = new MiruTopologyStatus(
                                    new MiruPartition(
                                        new MiruPartitionCoord(tenantId, c.getColumn().partitionId, host),
                                        new MiruPartitionCoordInfo(value.state, value.storage)), value.lastActiveTimestamp);

                                List<MiruTopologyStatus> statuses = topologies.get(tenantId, c.getColumn().partitionId);
                                if (statuses == null) {
                                    statuses = Lists.newArrayList();
                                    topologies.put(tenantId, c.getColumn().partitionId, statuses);
                                }
                                statuses.add(status);
                            }
                            return c;
                        }
                }));
        }
        if (!callbacks.isEmpty()) {
            topologyRegistry.multiRowGetAll(MiruVoidByte.INSTANCE, callbacks);
        }
        if (!topologies.isEmpty()) {
            for (Map.Entry<MiruTenantId, Map<MiruPartitionId, List<MiruTopologyStatus>>> tenantEntry : topologies.rowMap().entrySet()) {
                MiruTenantId tenantId = tenantEntry.getKey();
                Set<MiruPartitionId> partitionIds = tenantEntry.getValue().keySet();
                Map<MiruPartitionId, MiruReplicaSet> replicaSets = getReplicaSets(tenantId, partitionIds);
                for (Map.Entry<MiruPartitionId, List<MiruTopologyStatus>> partitionEntry : tenantEntry.getValue().entrySet()) {
                    MiruReplicaSet replicaSet = replicaSets.get(partitionEntry.getKey());
                    for (MiruTopologyStatus status : partitionEntry.getValue()) {
                        if (replicaSet.getHostsWithReplica().contains(status.partition.coord.host)) {
                            callbackStream.callback(status);
                        }
                    }
                }
            }
        }
        callbackStream.callback(null);
    }

    private ColumnValueAndTimestamp<MiruTopologyColumnKey, MiruTopologyColumnValue, Long> getTopologyValueAndTimestamp(
        MiruTenantId tenantId, MiruPartitionId partitionId, MiruHost host) throws Exception {

        MiruTopologyColumnKey key = new MiruTopologyColumnKey(partitionId, host);
        ColumnValueAndTimestamp<MiruTopologyColumnKey, MiruTopologyColumnValue, Long>[] valueAndTimestamps = topologyRegistry
            .multiGetEntries(MiruVoidByte.INSTANCE, tenantId, new MiruTopologyColumnKey[] { key }, null, null);
        if (valueAndTimestamps != null && valueAndTimestamps.length > 0 && valueAndTimestamps[0] != null) {
            ColumnValueAndTimestamp<MiruTopologyColumnKey, MiruTopologyColumnValue, Long> valueAndTimestamp = valueAndTimestamps[0];
            return valueAndTimestamp;
        } else {
            return null;
        }
    }

    private List<MiruTopologyStatus> getTopologyStatusByState(
        final MiruTenantConfig config,
        final MiruTenantId tenantId,
        final Optional<MiruHost> filterForHost) throws Exception {

        final long topologyIsStaleAfterMillis = config.getLong(MiruTenantConfigFields.topology_is_stale_after_millis.name(), defaultTopologyIsStaleAfterMillis);
        final int numberOfReplicas = config.getInt(MiruTenantConfigFields.number_of_replicas.name(), defaultNumberOfReplicas);

        final List<ColumnValueAndTimestamp<MiruTopologyColumnKey, MiruTopologyColumnValue, Long>> cvats = new ArrayList<>();
        final Set<MiruPartitionId> partitions = new HashSet<>();
        ColumnValueAndTimestamp<MiruTopologyColumnKey, MiruTopologyColumnValue, Long>[] result =
            topologyRegistry.multiGetEntries(MiruVoidByte.INSTANCE, tenantId, null, null, null);
        for (ColumnValueAndTimestamp<MiruTopologyColumnKey, MiruTopologyColumnValue, Long> cvat : result) {
            if (cvat != null) {
                MiruPartitionId partitionId = cvat.getColumn().partitionId;
                MiruHost host = cvat.getColumn().host;
                if (!filterForHost.isPresent() || filterForHost.get().equals(host)) {
                    cvats.add(cvat);
                    partitions.add(partitionId);
                }
            }
        }

        SetMultimap<MiruPartitionId, MiruHost> hostsWithReplica = getHostsWithReplica(tenantId, partitions, numberOfReplicas);
        return extractStatus(cvats, hostsWithReplica, tenantId, topologyIsStaleAfterMillis);
    }

    private Map<MiruPartitionId, List<MiruTopologyStatus>> getPartitionTopologyStatusByState(
        final MiruTenantConfig config,
        final MiruTenantId tenantId,
        final Collection<MiruPartitionId> requiredPartitionId,
        final SetMultimap<MiruPartitionId, MiruHost> perPartitonHostsWithReplica) throws Exception {

        final ListMultimap<MiruPartitionId, ColumnValueAndTimestamp<MiruTopologyColumnKey, MiruTopologyColumnValue, Long>> partitionsTopology =
            ArrayListMultimap.create();
        topologyRegistry.getEntrys(
            MiruVoidByte.INSTANCE, tenantId, null, null, 100, false, null, null,
            new CallbackStream<ColumnValueAndTimestamp<MiruTopologyColumnKey, MiruTopologyColumnValue, Long>>() {
                @Override
                public ColumnValueAndTimestamp<MiruTopologyColumnKey, MiruTopologyColumnValue, Long> callback(
                    ColumnValueAndTimestamp<MiruTopologyColumnKey, MiruTopologyColumnValue, Long> cvat) throws Exception {

                        if (cvat != null) {
                            MiruPartitionId partitionId = cvat.getColumn().partitionId;
                            partitionsTopology.put(partitionId, cvat);
                        }
                        return cvat;
                    }
            });

        long topologyIsStaleAfterMillis = config.getLong(MiruTenantConfigFields.topology_is_stale_after_millis.name(), defaultTopologyIsStaleAfterMillis);
        Map<MiruPartitionId, List<MiruTopologyStatus>> result = new HashMap<>();
        for (MiruPartitionId miruPartitionId : requiredPartitionId) {
            result.put(miruPartitionId, extractStatus(partitionsTopology.get(miruPartitionId), perPartitonHostsWithReplica, tenantId,
                topologyIsStaleAfterMillis));
        }
        return result;
    }

    private List<MiruTopologyStatus> extractStatus(
        List<ColumnValueAndTimestamp<MiruTopologyColumnKey, MiruTopologyColumnValue, Long>> cvats,
        final SetMultimap<MiruPartitionId, MiruHost> perPartitonHostsWithReplica,
        final MiruTenantId tenantId,
        final long topologyIsStaleAfterMillis) {

        List<MiruTopologyStatus> statuses = Lists.newArrayList();
        for (ColumnValueAndTimestamp<MiruTopologyColumnKey, MiruTopologyColumnValue, Long> cvat : cvats) {
            MiruPartitionId partitionId = cvat.getColumn().partitionId;
            MiruHost host = cvat.getColumn().host;
            Set<MiruHost> hostsWithReplica = perPartitonHostsWithReplica.get(partitionId);
            if (hostsWithReplica.contains(host)) {
                MiruTopologyColumnValue value = cvat.getValue();
                MiruPartitionCoord coord = new MiruPartitionCoord(tenantId, partitionId, host);
                MiruPartitionState state = normalizeState(value.state, value.lastActiveTimestamp, topologyIsStaleAfterMillis);
                MiruBackingStorage storage = value.storage;
                MiruPartitionCoordInfo coordInfo = new MiruPartitionCoordInfo(state, storage);
                statuses.add(new MiruTopologyStatus(new MiruPartition(coord, coordInfo), value.lastActiveTimestamp));
            }
        }
        return statuses;
    }

    private MiruPartitionState normalizeState(MiruPartitionState state, long timestamp, long topologyIsStaleAfterMillis) {
        if (state != MiruPartitionState.offline && timestamp < (timestamper.get() - topologyIsStaleAfterMillis)) {
            return MiruPartitionState.offline;
        } else {
            return state;
        }
    }

    private MiruPartitionActive isTenantPartitionActiveForHost(MiruTenantId tenantId,
        MiruPartitionId partitionId,
        MiruHost host,
        MiruTenantConfig config)
        throws Exception {

        MiruTopologyColumnKey key = new MiruTopologyColumnKey(partitionId, host);
        ColumnValueAndTimestamp<MiruTopologyColumnKey, MiruTopologyColumnValue, Long>[] valueAndTimestamps = topologyRegistry
            .multiGetEntries(MiruVoidByte.INSTANCE, tenantId, new MiruTopologyColumnKey[]{key}, null, null);

        long topologyTimestamp = -1;
        if (valueAndTimestamps != null && valueAndTimestamps.length > 0 && valueAndTimestamps[0] != null) {
            topologyTimestamp = valueAndTimestamps[0].getValue().lastActiveTimestamp;
        }
        long topologyIsStaleAfterMillis = config.getLong(MiruTenantConfigFields.topology_is_stale_after_millis.name(), defaultTopologyIsStaleAfterMillis);
        boolean active = topologyTimestamp > -1 && (topologyTimestamp + topologyIsStaleAfterMillis) > timestamper.get();
        boolean idle = topologyTimestamp > -1 && (topologyTimestamp + defaultTopologyIsIdleAfterMillis) < timestamper.get();
        return new MiruPartitionActive(active, idle);
    }

    private final Function<MiruTopologyStatus, MiruPartition> topologyStatusToPartition = new Function<MiruTopologyStatus, MiruPartition>() {
        @Override
        public MiruPartition apply(MiruTopologyStatus input) {
            return input.partition;
        }
    };

    @Override
    public void addToReplicaRegistry(MiruTenantId tenantId, MiruPartitionId partitionId, long nextId, MiruHost host)
        throws Exception {
        replicaRegistry.add(tenantId, partitionId, nextId, host, null, timestamper);
        markTenantTopologyUpdated(Arrays.asList(tenantId));
    }

    @Override
    public void removeTenantPartionReplicaSet(MiruTenantId tenantId, MiruPartitionId partitionId) throws Exception {
        replicaRegistry.removeRow(tenantId, partitionId, timestamper);
        markTenantTopologyUpdated(Arrays.asList(tenantId));
    }

    @Override
    public void ensurePartitionCoord(MiruPartitionCoord coord) throws Exception {
        expectedTenantsRegistry.add(MiruVoidByte.INSTANCE, coord.host, coord.tenantId, MiruVoidByte.INSTANCE, null, timestamper);
        expectedTenantPartitionsRegistry.add(coord.tenantId, coord.host, coord.partitionId, MiruVoidByte.INSTANCE, null, timestamper);
        topologyRegistry.addIfNotExists(MiruVoidByte.INSTANCE, coord.tenantId,
            new MiruTopologyColumnKey(coord.partitionId, coord.host),
            new MiruTopologyColumnValue(MiruPartitionState.offline, MiruBackingStorage.memory, 0),
            null, timestamper);
        log.debug("Expecting {}", coord);
    }

    @Override
    public MiruSchema getSchema(MiruTenantId tenantId) throws Exception {
        final AtomicReference<MiruSchema> schema = new AtomicReference<>();
        schemaRegistry.getValues(MiruVoidByte.INSTANCE, tenantId, new MiruSchemaColumnKey(null, Long.MAX_VALUE), 1L, 1, false, null, null,
            new CallbackStream<MiruSchema>() {
                @Override
                public MiruSchema callback(MiruSchema miruSchema) throws Exception {
                    if (miruSchema != null) {
                        schema.set(miruSchema);
                    }
                    return null; // always done after one
                }
            });
        return schema.get();
    }

    @Override
    public void registerSchema(MiruTenantId tenantId, MiruSchema schema) throws Exception {
        schemaRegistry.add(MiruVoidByte.INSTANCE, tenantId, new MiruSchemaColumnKey(schema.getName(), schema.getVersion()), schema, null, null);
    }

    private static class TenantPartitionHostKey {

        private final MiruTenantId tenantId;
        private final MiruPartitionId partitionId;
        private final MiruHost host;

        private TenantPartitionHostKey(MiruTenantId tenantId, MiruPartitionId partitionId, MiruHost host) {
            this.tenantId = tenantId;
            this.partitionId = partitionId;
            this.host = host;
        }

        TenantPartitionHostKey(MiruPartitionCoord coord) {
            this(coord.tenantId, coord.partitionId, coord.host);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            TenantPartitionHostKey that = (TenantPartitionHostKey) o;

            if (host != null ? !host.equals(that.host) : that.host != null) {
                return false;
            }
            if (partitionId != null ? !partitionId.equals(that.partitionId) : that.partitionId != null) {
                return false;
            }
            return !(tenantId != null ? !tenantId.equals(that.tenantId) : that.tenantId != null);
        }

        @Override
        public int hashCode() {
            int result = tenantId != null ? tenantId.hashCode() : 0;
            result = 31 * result + (partitionId != null ? partitionId.hashCode() : 0);
            result = 31 * result + (host != null ? host.hashCode() : 0);
            return result;
        }
    }
}
