package com.jivesoftware.os.miru.cluster.amza;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.MinMaxPriorityQueue;
import com.google.common.collect.Multimaps;
import com.google.common.collect.SetMultimap;
import com.jivesoftware.os.amza.service.AmzaService;
import com.jivesoftware.os.amza.service.AmzaTable;
import com.jivesoftware.os.amza.shared.RowIndexKey;
import com.jivesoftware.os.amza.shared.RowIndexValue;
import com.jivesoftware.os.amza.shared.RowScan;
import com.jivesoftware.os.amza.shared.TableName;
import com.jivesoftware.os.filer.io.FilerIO;
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
import com.jivesoftware.os.miru.api.topology.HostHeartbeat;
import com.jivesoftware.os.miru.api.topology.MiruPartitionActive;
import com.jivesoftware.os.miru.api.topology.MiruTenantConfig;
import com.jivesoftware.os.miru.api.topology.MiruTenantTopologyUpdate;
import com.jivesoftware.os.miru.cluster.MiruClusterRegistry;
import com.jivesoftware.os.miru.cluster.MiruReplicaSet;
import com.jivesoftware.os.miru.cluster.MiruTenantConfigFields;
import com.jivesoftware.os.miru.cluster.marshaller.MiruHostMarshaller;
import com.jivesoftware.os.miru.cluster.marshaller.MiruTopologyColumnValueMarshaller;
import com.jivesoftware.os.miru.cluster.rcvs.MiruTopologyColumnValue;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import com.jivesoftware.os.rcvs.marshall.api.TypeMarshaller;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.TimeUnit;

/**
 *
 * @author jonathan.colt
 */
public class AmzaClusterRegistry implements MiruClusterRegistry {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    private static final byte[] EMPTY_BYTES = new byte[0];

    private final MiruHostMarshaller hostMarshaller = new MiruHostMarshaller();
    private final MiruTopologyColumnValueMarshaller topologyColumnValueMarshaller = new MiruTopologyColumnValueMarshaller();
    private final TypeMarshaller<MiruSchema> schemaMarshaller;

    private final StripingLocksProvider<MiruPartitionCoord> topologyLocks = new StripingLocksProvider<>(64);
    private final AmzaTable hosts;
    private final AmzaTable schemas;

    private final AmzaService amzaService;
    private final int defaultNumberOfReplicas;
    private final long defaultTopologyIsStaleAfterMillis;
    private final long defaultTopologyIsIdleAfterMillis;

    public AmzaClusterRegistry(AmzaService amzaService,
        TypeMarshaller<MiruSchema> schemaMarshaller,
        int defaultNumberOfReplicas,
        long defaultTopologyIsStaleAfterMillis,
        long defaultTopologyIsIdleAfterMillis) throws Exception {
        this.amzaService = amzaService;
        this.schemaMarshaller = schemaMarshaller;
        this.defaultNumberOfReplicas = defaultNumberOfReplicas;
        this.defaultTopologyIsStaleAfterMillis = defaultTopologyIsStaleAfterMillis;
        this.defaultTopologyIsIdleAfterMillis = defaultTopologyIsIdleAfterMillis;
        this.hosts = amzaService.getTable(new TableName("master", "hosts", null, null));
        this.schemas = amzaService.getTable(new TableName("master", "schemas", null, null));
    }

    @Override
    public void sendHeartbeatForHost(MiruHost miruHost) throws Exception {
        byte[] key = hostMarshaller.toBytes(miruHost);
        hosts.set(new RowIndexKey(key), FilerIO.longBytes(System.currentTimeMillis()));
    }

    @Override
    public LinkedHashSet<HostHeartbeat> getAllHosts() throws Exception {
        final LinkedHashSet<HostHeartbeat> heartbeats = new LinkedHashSet<>();
        hosts.scan(new RowScan<Exception>() {

            @Override
            public boolean row(long transactionId, RowIndexKey key, RowIndexValue value) throws Exception {
                if (!value.getTombstoned()) { // paranoid
                    MiruHost host = hostMarshaller.fromBytes(key.getKey());
                    long timestamp = FilerIO.bytesLong(value.getValue());
                    heartbeats.add(new HostHeartbeat(host, timestamp));
                }
                return true;
            }
        });
        return heartbeats;
    }

    private RowIndexKey topologyKey(MiruTenantId tenantId, MiruPartitionId partitionId) {
        int length = tenantId.length();
        ByteBuffer bb = ByteBuffer.allocate(length + 4 + 4);
        bb.put(tenantId.getBytes());
        bb.putInt(partitionId.getId());
        bb.putInt(length);
        return new RowIndexKey(bb.array());
    }

    private TenantAndPartition topologyKey(RowIndexKey rowIndexKey) {
        ByteBuffer bb = ByteBuffer.wrap(rowIndexKey.getKey());
        bb.position(bb.limit() - 4);
        int length = bb.getInt();
        bb.position(0);
        byte[] tenantBytes = new byte[length];
        bb.get(tenantBytes);
        return new TenantAndPartition(tenantBytes, bb.getInt());
    }

    static class TenantAndPartition {

        public final byte[] tenantBytes;
        public final int partitionId;

        public TenantAndPartition(byte[] miruTenantId, int miruPartitionId) {
            this.tenantBytes = miruTenantId;
            this.partitionId = miruPartitionId;
        }

    }

    @Override
    public void updateTopology(MiruPartitionCoord coord, Optional<MiruPartitionCoordInfo> optionalInfo, Optional<Long> refreshTimestamp) throws Exception {

        AmzaTable topology = amzaService.getTable(new TableName("master", "host-" + coord.host.toStringForm() + "-partition-info", null, null));
        RowIndexKey key = topologyKey(coord.tenantId, coord.partitionId);

        synchronized (topologyLocks.lock(coord)) {
            MiruPartitionCoordInfo coordInfo;
            if (optionalInfo.isPresent()) {
                coordInfo = optionalInfo.get();
            } else {
                byte[] got = topology.get(key);
                if (got != null) {
                    MiruTopologyColumnValue value = topologyColumnValueMarshaller.fromBytes(got);
                    coordInfo = new MiruPartitionCoordInfo(value.state, value.storage);
                } else {
                    coordInfo = new MiruPartitionCoordInfo(MiruPartitionState.offline, MiruBackingStorage.memory);
                }
            }
            long timestamp;
            if (refreshTimestamp.isPresent()) {
                timestamp = refreshTimestamp.get();
            } else {
                byte[] got = topology.get(key);
                if (got != null) {
                    MiruTopologyColumnValue value = topologyColumnValueMarshaller.fromBytes(got);
                    timestamp = value.lastActiveTimestamp;
                } else {
                    timestamp = 0;
                }
            }

            MiruTopologyColumnValue update = new MiruTopologyColumnValue(coordInfo.state, coordInfo.storage, timestamp);
            topology.set(key, topologyColumnValueMarshaller.toBytes(update));
            LOG.debug("Updated {} to {} at {}", new Object[]{coord, coordInfo, refreshTimestamp});
        }

        markTenantTopologyUpdated(Arrays.asList(coord.tenantId));
    }

    private void markTenantTopologyUpdated(List<MiruTenantId> tenantIds) throws Exception {
        for (HostHeartbeat heartbeat : getAllHosts()) {
            AmzaTable topology = amzaService.getTable(new TableName("master", "host-" + heartbeat.host.toStringForm() + "-topology-updates", null, null));
            for (MiruTenantId tenantId : tenantIds) {
                topology.set(new RowIndexKey(tenantId.getBytes()), EMPTY_BYTES);
            }
        }
    }

    @Override
    public List<MiruTenantTopologyUpdate> getTopologyUpdatesForHost(MiruHost host, long sinceTimestamp) throws Exception {
        final List<MiruTenantTopologyUpdate> updates = Lists.newArrayList();
        final long acceptableTimestampId = amzaService.getTimestamp(sinceTimestamp, TimeUnit.MINUTES.toMillis(1));

        AmzaTable topology = amzaService.getTable(new TableName("master", "host-" + host.toStringForm() + "-topology-updates", null, null));
        topology.scan(new RowScan<Exception>() {
            @Override
            public boolean row(long transactionId, RowIndexKey key, RowIndexValue value) throws Exception {
                if (value.getTimestampId() > acceptableTimestampId) {
                    updates.add(new MiruTenantTopologyUpdate(new MiruTenantId(key.getKey()), value.getTimestampId()));
                }
                return true;
            }
        });

        return updates;
    }

    @Override
    public void ensurePartitionCoord(MiruPartitionCoord coord) throws Exception {

        AmzaTable topology = amzaService.getTable(new TableName("master", "host-" + coord.host.toStringForm() + "-partition-info", null, null));
        RowIndexKey key = topologyKey(coord.tenantId, coord.partitionId);
        if (topology.get(key) == null) { // TODO don't have a set if absent. This is a little racy
            MiruTopologyColumnValue update = new MiruTopologyColumnValue(MiruPartitionState.offline, MiruBackingStorage.memory, 0);
            topology.set(key, topologyColumnValueMarshaller.toBytes(update));
        }

    }

    @Override
    public void addToReplicaRegistry(MiruTenantId tenantId, MiruPartitionId partitionId, long nextId, MiruHost host) throws Exception {
        AmzaTable topology = amzaService.getTable(new TableName("master", "host-" + host.toStringForm() + "-partition-registry", null, null));
        RowIndexKey key = topologyKey(tenantId, partitionId);
        topology.set(key, FilerIO.longBytes(nextId)); // most recent is smallest.
        markTenantTopologyUpdated(Arrays.asList(tenantId));
    }

    @Override
    public MiruTenantConfig getTenantConfig(MiruTenantId tenantId) throws Exception {
        Map<String, Long> config = Maps.newHashMap();
        LOG.error("Unimplemented getTenantConfig in " + this.getClass().getSimpleName());
        return new MiruTenantConfig(config);
    }

    @Override
    public int getNumberOfReplicas(MiruTenantId tenantId) throws Exception {
        return getTenantConfig(tenantId).getInt(MiruTenantConfigFields.number_of_replicas.name(), defaultNumberOfReplicas);
    }

    @Override
    public List<MiruTenantId> getTenantsForHost(MiruHost host) throws Exception {
        final Set<MiruTenantId> tenants = new HashSet<>();
        AmzaTable topology = amzaService.getTable(new TableName("master", "host-" + host.toStringForm() + "-partition-registry", null, null));
        topology.scan(new RowScan<Exception>() {

            @Override
            public boolean row(long transactionId, RowIndexKey key, RowIndexValue value) throws Exception {
                TenantAndPartition topologyKey = topologyKey(key);
                tenants.add(new MiruTenantId(topologyKey.tenantBytes));
                return true;
            }
        });
        return new ArrayList<>(tenants);
    }

    @Override
    public void removeTenantPartionReplicaSet(final MiruTenantId tenantId, final MiruPartitionId partitionId) throws Exception {
        hosts.scan(new RowScan<Exception>() {

            @Override
            public boolean row(long transactionId, RowIndexKey key, RowIndexValue value) throws Exception {
                if (!value.getTombstoned()) { // paranoid
                    MiruHost host = hostMarshaller.fromBytes(key.getKey());
                    AmzaTable topologyReg = amzaService.getTable(new TableName("master", "host-" + host.toStringForm() + "-partition-registry", null, null));
                    AmzaTable topologyInfo = amzaService.getTable(new TableName("master", "host-" + host.toStringForm() + "-partition-info", null, null));
                    RowIndexKey topologyKey = topologyKey(tenantId, partitionId);
                    topologyReg.remove(topologyKey);
                    topologyInfo.remove(topologyKey);
                }
                return true;
            }
        });
        markTenantTopologyUpdated(Arrays.asList(tenantId));
    }

    @Override
    public List<MiruPartition> getPartitionsForTenant(MiruTenantId tenantId) throws Exception {
        ConcurrentSkipListMap<Integer, MinMaxPriorityQueue<HostAndTimestamp>> paritionIdToLatest = tenantsLatestTopology(tenantId);

        List<MiruPartition> partitions = new ArrayList<>();
        for (int partitionId : paritionIdToLatest.keySet()) {
            MiruPartitionId miruPartitionId = MiruPartitionId.of(partitionId);
            MinMaxPriorityQueue<HostAndTimestamp> got = paritionIdToLatest.get(partitionId);
            for (HostAndTimestamp hat : got) {
                AmzaTable topologyInfo = amzaService.getTable(new TableName("master", "host-" + hat.host.toStringForm() + "-partition-info", null, null));
                byte[] rawInfo = topologyInfo.get(topologyKey(tenantId, miruPartitionId));
                MiruPartitionCoordInfo info;
                if (rawInfo == null) {
                    info = new MiruPartitionCoordInfo(MiruPartitionState.offline, MiruBackingStorage.memory);
                } else {
                    MiruTopologyColumnValue columnValue = topologyColumnValueMarshaller.fromBytes(rawInfo);
                    info = new MiruPartitionCoordInfo(columnValue.state, columnValue.storage);
                }
                partitions.add(new MiruPartition(new MiruPartitionCoord(tenantId, miruPartitionId, hat.host), info));
            }
        }
        return partitions;
    }

    private ConcurrentSkipListMap<Integer, MinMaxPriorityQueue<HostAndTimestamp>> tenantsLatestTopology(MiruTenantId tenantId) throws Exception {
        final RowIndexKey from = new RowIndexKey(tenantId.getBytes());
        final RowIndexKey to = new RowIndexKey(prefixUpperExclusive(tenantId.getBytes()));
        final ConcurrentSkipListMap<Integer, MinMaxPriorityQueue<HostAndTimestamp>> partitionIdToLatest = new ConcurrentSkipListMap<>();
        hosts.scan(new RowScan<Exception>() {

            @Override
            public boolean row(long transactionId, RowIndexKey key, RowIndexValue value) throws Exception {
                if (!value.getTombstoned()) { // paranoid
                    final MiruHost host = hostMarshaller.fromBytes(key.getKey());
                    AmzaTable topologyReg = amzaService.getTable(new TableName("master", "host-" + host.toStringForm() + "-partition-registry", null, null));
                    topologyReg.rangeScan(from, to, new RowScan<Exception>() {

                        @Override
                        public boolean row(long transactionId, RowIndexKey key, RowIndexValue value) throws Exception {
                            TenantAndPartition topologyKey = topologyKey(key);
                            MinMaxPriorityQueue<HostAndTimestamp> got = partitionIdToLatest.get(topologyKey.partitionId);
                            if (got == null) {
                                // TODO defaultNumberOfReplicas should come from config?
                                got = MinMaxPriorityQueue.maximumSize(defaultNumberOfReplicas).expectedSize(defaultNumberOfReplicas).create();
                                partitionIdToLatest.put(topologyKey.partitionId, got);
                            }
                            got.add(new HostAndTimestamp(host, FilerIO.bytesLong(value.getValue())));
                            return true;
                        }
                    });
                }
                return true;
            }
        });
        return partitionIdToLatest;
    }

    public byte[] prefixUpperExclusive(byte[] preBytes) {
        byte[] raw = new byte[preBytes.length];
        System.arraycopy(preBytes, 0, raw, 0, preBytes.length);

        // given: [64,72,96,127]
        // want: [64,72,97,-128]
        for (int i = raw.length - 1; i >= 0; i--) {
            if (raw[i] == Byte.MAX_VALUE) {
                raw[i] = Byte.MIN_VALUE;
            } else {
                raw[i]++;
                break;
            }
        }
        return raw;
    }

    static class HostAndTimestamp implements Comparable<HostAndTimestamp> {

        public final MiruHost host;
        public final long timestamp;

        public HostAndTimestamp(MiruHost host, long timestamp) {
            this.host = host;
            this.timestamp = timestamp;
        }

        @Override
        public int compareTo(HostAndTimestamp o) {
            return Long.compare(timestamp, o.timestamp);
        }

    }

    @Override
    public List<MiruPartition> getPartitionsForTenantHost(MiruTenantId tenantId, MiruHost host) throws Exception {
        ConcurrentSkipListMap<Integer, MinMaxPriorityQueue<HostAndTimestamp>> partitionIdToLatest = tenantsLatestTopology(tenantId);
        List<MiruPartition> partitions = new ArrayList<>();
        for (int partitionId : partitionIdToLatest.keySet()) {
            MiruPartitionId miruPartitionId = MiruPartitionId.of(partitionId);
            MinMaxPriorityQueue<HostAndTimestamp> got = partitionIdToLatest.get(partitionId);
            for (HostAndTimestamp hat : got) {
                if (hat.host.equals(host)) {
                    AmzaTable topologyInfo = amzaService.getTable(new TableName("master", "host-" + hat.host.toStringForm() + "-partition-info", null, null));
                    byte[] rawInfo = topologyInfo.get(topologyKey(tenantId, miruPartitionId));
                    MiruPartitionCoordInfo info;
                    if (rawInfo == null) {
                        info = new MiruPartitionCoordInfo(MiruPartitionState.offline, MiruBackingStorage.memory);
                    } else {
                        MiruTopologyColumnValue columnValue = topologyColumnValueMarshaller.fromBytes(rawInfo);
                        info = new MiruPartitionCoordInfo(columnValue.state, columnValue.storage);
                    }
                    partitions.add(new MiruPartition(new MiruPartitionCoord(tenantId, miruPartitionId, hat.host), info));
                }
            }
        }
        return partitions;
    }

    @Override
    public List<MiruTopologyStatus> getTopologyStatusForTenant(MiruTenantId tenantId) throws Exception {
        ConcurrentSkipListMap<Integer, MinMaxPriorityQueue<HostAndTimestamp>> partitionIdToLatest = tenantsLatestTopology(tenantId);
        List<MiruTopologyStatus> status = new ArrayList<>();
        for (int partitionId : partitionIdToLatest.keySet()) {
            MiruPartitionId miruPartitionId = MiruPartitionId.of(partitionId);
            MinMaxPriorityQueue<HostAndTimestamp> got = partitionIdToLatest.get(partitionId);
            for (HostAndTimestamp hat : got) {
                AmzaTable topologyInfo = amzaService.getTable(new TableName("master", "host-" + hat.host.toStringForm() + "-partition-info", null, null));
                byte[] rawInfo = topologyInfo.get(topologyKey(tenantId, miruPartitionId));
                MiruPartitionCoordInfo info;
                long lastActiveTimestampMillis = 0;
                if (rawInfo == null) {
                    info = new MiruPartitionCoordInfo(MiruPartitionState.offline, MiruBackingStorage.memory);
                } else {
                    MiruTopologyColumnValue columnValue = topologyColumnValueMarshaller.fromBytes(rawInfo);
                    info = new MiruPartitionCoordInfo(columnValue.state, columnValue.storage);
                    lastActiveTimestampMillis = columnValue.lastActiveTimestamp;
                }
                MiruPartition miruPartition = new MiruPartition(new MiruPartitionCoord(tenantId, miruPartitionId, hat.host), info);
                status.add(new MiruTopologyStatus(miruPartition, lastActiveTimestampMillis));
            }
        }
        return status;
    }

    @Override
    public List<MiruTopologyStatus> getTopologyStatusForTenantHost(MiruTenantId tenantId, MiruHost host) throws Exception {
        ConcurrentSkipListMap<Integer, MinMaxPriorityQueue<HostAndTimestamp>> partitionIdToLatest = tenantsLatestTopology(tenantId);
        List<MiruTopologyStatus> status = new ArrayList<>();
        for (int partitionId : partitionIdToLatest.keySet()) {
            MiruPartitionId miruPartitionId = MiruPartitionId.of(partitionId);
            MinMaxPriorityQueue<HostAndTimestamp> got = partitionIdToLatest.get(partitionId);
            for (HostAndTimestamp hat : got) {
                if (hat.host.equals(host)) {
                    AmzaTable topologyInfo = amzaService.getTable(new TableName("master", "host-" + hat.host.toStringForm() + "-partition-info", null, null));
                    byte[] rawInfo = topologyInfo.get(topologyKey(tenantId, miruPartitionId));
                    MiruPartitionCoordInfo info;
                    long lastActiveTimestampMillis = 0;
                    if (rawInfo == null) {
                        info = new MiruPartitionCoordInfo(MiruPartitionState.offline, MiruBackingStorage.memory);
                    } else {
                        MiruTopologyColumnValue columnValue = topologyColumnValueMarshaller.fromBytes(rawInfo);
                        info = new MiruPartitionCoordInfo(columnValue.state, columnValue.storage);
                        lastActiveTimestampMillis = columnValue.lastActiveTimestamp;
                    }
                    MiruPartition miruPartition = new MiruPartition(new MiruPartitionCoord(tenantId, miruPartitionId, hat.host), info);
                    status.add(new MiruTopologyStatus(miruPartition, lastActiveTimestampMillis));
                }
            }
        }
        return status;
    }

    @Override
    public Map<MiruPartitionId, MiruReplicaSet> getReplicaSets(MiruTenantId tenantId, Collection<MiruPartitionId> requiredPartitionId) throws Exception {
        ConcurrentSkipListMap<Integer, MinMaxPriorityQueue<HostAndTimestamp>> partitionIdToLatest = tenantsLatestTopology(tenantId);
        ListMultimap<Integer, MiruPartition> partitionsPartitions = ArrayListMultimap.create();
        SetMultimap<Integer, MiruHost> partitionHosts = HashMultimap.create();

        for (int partitionId : partitionIdToLatest.keySet()) {
            MiruPartitionId miruPartitionId = MiruPartitionId.of(partitionId);
            if (requiredPartitionId.contains(miruPartitionId)) {
                MinMaxPriorityQueue<HostAndTimestamp> got = partitionIdToLatest.get(partitionId);
                for (HostAndTimestamp hat : got) {
                    AmzaTable topologyInfo = amzaService.getTable(new TableName("master", "host-" + hat.host.toStringForm() + "-partition-info", null, null));
                    byte[] rawInfo = topologyInfo.get(topologyKey(tenantId, miruPartitionId));
                    MiruPartitionCoordInfo info;
                    if (rawInfo == null) {
                        info = new MiruPartitionCoordInfo(MiruPartitionState.offline, MiruBackingStorage.memory);
                    } else {
                        MiruTopologyColumnValue columnValue = topologyColumnValueMarshaller.fromBytes(rawInfo);
                        info = new MiruPartitionCoordInfo(columnValue.state, columnValue.storage);
                    }
                    partitionsPartitions.put(partitionId, new MiruPartition(new MiruPartitionCoord(tenantId, miruPartitionId, hat.host), info));
                    partitionHosts.put(partitionId, hat.host);
                }
            }
        }

        Map<MiruPartitionId, MiruReplicaSet> replicaSet = new HashMap<>();
        for (MiruPartitionId partitionId : requiredPartitionId) {
            List<MiruPartition> partitions = partitionsPartitions.get(partitionId.getId());
            Set<MiruHost> replicaHosts = partitionHosts.get(partitionId.getId());
            int missing = defaultNumberOfReplicas - replicaHosts.size(); // TODO expose to config?
            replicaSet.put(partitionId, new MiruReplicaSet(extractPartitionsByState(partitions), replicaHosts, missing));
        }
        return replicaSet;
    }

    private ListMultimap<MiruPartitionState, MiruPartition> extractPartitionsByState(List<MiruPartition> partitions) {
        return Multimaps.index(partitions, new Function<MiruPartition, MiruPartitionState>() {
            @Override
            public MiruPartitionState apply(MiruPartition input) {
                return input.info.state;
            }
        });
    }

    @Override
    public MiruPartitionActive isPartitionActive(MiruPartitionCoord coord) throws Exception {
        AmzaTable topologyInfo = amzaService.getTable(new TableName("master", "host-" + coord.host.toStringForm() + "-partition-info", null, null));
        byte[] got = topologyInfo.get(topologyKey(coord.tenantId, coord.partitionId));
        if (got == null) {
            return new MiruPartitionActive(false, false);
        } else {
            MiruTopologyColumnValue columnValue = topologyColumnValueMarshaller.fromBytes(got);
            long now = System.currentTimeMillis();
            return new MiruPartitionActive(columnValue.lastActiveTimestamp + defaultTopologyIsStaleAfterMillis > now,
                columnValue.lastActiveTimestamp + defaultTopologyIsIdleAfterMillis < now);
        }
    }

    @Override
    public void removeHost(MiruHost host) throws Exception {
        List<MiruTenantId> tenantIds = getTenantsForHost(host);

        // TODO add to Amza removeTable
        hosts.remove(new RowIndexKey(hostMarshaller.toBytes(host)));

        markTenantTopologyUpdated(tenantIds);
    }

    @Override
    public void removeTopology(MiruTenantId tenantId, MiruPartitionId partitionId, MiruHost host) throws Exception {
        AmzaTable topologyInfo = amzaService.getTable(new TableName("master", "host-" + host.toStringForm() + "-partition-info", null, null));
        topologyInfo.remove(topologyKey(tenantId, partitionId));
        markTenantTopologyUpdated(Arrays.asList(tenantId));
    }

    @Override
    public void topologiesForTenants(List<MiruTenantId> tenantIds, CallbackStream<MiruTopologyStatus> callbackStream) throws Exception {
        for (MiruTenantId tenantId : tenantIds) {
            List<MiruTopologyStatus> statuses = getTopologyStatusForTenant(tenantId);
            for (MiruTopologyStatus status : statuses) {
                if (callbackStream.callback(status) != status) {
                    return;
                }
            }
        }
        callbackStream.callback(null); //EOS
    }

    @Override
    public MiruSchema getSchema(MiruTenantId tenantId) throws Exception {
        byte[] got = schemas.get(new RowIndexKey(tenantId.getBytes()));
        if (got == null) {
            return null;
        }
        return schemaMarshaller.fromBytes(got);
    }

    @Override
    public void registerSchema(MiruTenantId tenantId, MiruSchema schema) throws Exception {
        schemas.set(new RowIndexKey(tenantId.getBytes()), schemaMarshaller.toBytes(schema));
    }

}
