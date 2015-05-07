package com.jivesoftware.os.miru.cluster.amza;

import com.google.common.base.Optional;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.MinMaxPriorityQueue;
import com.google.common.collect.Multimaps;
import com.google.common.collect.SetMultimap;
import com.google.common.collect.Sets;
import com.jivesoftware.os.amza.service.AmzaRegion;
import com.jivesoftware.os.amza.service.AmzaService;
import com.jivesoftware.os.amza.service.storage.RegionProvider;
import com.jivesoftware.os.amza.shared.PrimaryIndexDescriptor;
import com.jivesoftware.os.amza.shared.RegionName;
import com.jivesoftware.os.amza.shared.RegionProperties;
import com.jivesoftware.os.amza.shared.RingHost;
import com.jivesoftware.os.amza.shared.RowChanges;
import com.jivesoftware.os.amza.shared.RowsChanged;
import com.jivesoftware.os.amza.shared.TakeCursors;
import com.jivesoftware.os.amza.shared.WALKey;
import com.jivesoftware.os.amza.shared.WALStorageDescriptor;
import com.jivesoftware.os.filer.io.FilerIO;
import com.jivesoftware.os.jive.utils.base.interfaces.CallbackStream;
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
import com.jivesoftware.os.miru.api.topology.MiruPartitionActiveUpdate;
import com.jivesoftware.os.miru.api.topology.MiruTenantConfig;
import com.jivesoftware.os.miru.api.topology.MiruTenantTopologyUpdate;
import com.jivesoftware.os.miru.api.topology.NamedCursor;
import com.jivesoftware.os.miru.api.topology.NamedCursorsResult;
import com.jivesoftware.os.miru.api.wal.MiruWALClient;
import com.jivesoftware.os.miru.cluster.MiruClusterRegistry;
import com.jivesoftware.os.miru.cluster.MiruReplicaSet;
import com.jivesoftware.os.miru.cluster.MiruTenantConfigFields;
import com.jivesoftware.os.miru.cluster.MiruTenantPartitionRangeProvider;
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
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

/**
 * @author jonathan.colt
 */
public class AmzaClusterRegistry implements MiruClusterRegistry, RowChanges {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    private static final byte[] EMPTY_BYTES = new byte[0];
    private static final String AMZA_RING_NAME = "clusterRegistry";

    private final MiruHostMarshaller hostMarshaller = new MiruHostMarshaller();
    private final MiruTopologyColumnValueMarshaller topologyColumnValueMarshaller = new MiruTopologyColumnValueMarshaller();
    private final TypeMarshaller<MiruSchema> schemaMarshaller;

    private final AmzaService amzaService;
    private final MiruTenantPartitionRangeProvider rangeProvider;
    private final int defaultNumberOfReplicas;
    private final long defaultTopologyIsStaleAfterMillis;
    private final long defaultTopologyIsIdleAfterMillis;
    private final long defaultTopologyDestroyAfterMillis;
    private final int replicationFactor;
    private final int takeFromFactor;
    private final WALStorageDescriptor amzaStorageDescriptor;

    private final AtomicBoolean ringInitialized = new AtomicBoolean(false);

    public AmzaClusterRegistry(AmzaService amzaService,
        MiruTenantPartitionRangeProvider rangeProvider,
        TypeMarshaller<MiruSchema> schemaMarshaller,
        int defaultNumberOfReplicas,
        long defaultTopologyIsStaleAfterMillis,
        long defaultTopologyIsIdleAfterMillis,
        long defaultTopologyDestroyAfterMillis,
        int replicationFactor,
        int takeFromFactor) throws Exception {
        this.amzaService = amzaService;
        this.rangeProvider = rangeProvider;
        this.schemaMarshaller = schemaMarshaller;
        this.defaultNumberOfReplicas = defaultNumberOfReplicas;
        this.defaultTopologyIsStaleAfterMillis = defaultTopologyIsStaleAfterMillis;
        this.defaultTopologyIsIdleAfterMillis = defaultTopologyIsIdleAfterMillis;
        this.defaultTopologyDestroyAfterMillis = defaultTopologyDestroyAfterMillis;
        this.replicationFactor = replicationFactor;
        this.takeFromFactor = takeFromFactor;
        this.amzaStorageDescriptor = new WALStorageDescriptor(
            new PrimaryIndexDescriptor("berkeleydb", 0, false, null),
            null, 1000, 1000); //TODO config
    }

    @Override
    public void sendHeartbeatForHost(MiruHost miruHost) throws Exception {
        byte[] key = hostMarshaller.toBytes(miruHost);
        createRegionIfAbsent("hosts").set(new WALKey(key), FilerIO.longBytes(System.currentTimeMillis()));
    }

    @Override
    public LinkedHashSet<HostHeartbeat> getAllHosts() throws Exception {
        final LinkedHashSet<HostHeartbeat> heartbeats = new LinkedHashSet<>();
        createRegionIfAbsent("hosts").scan((transactionId, key, value) -> {
            if (!value.getTombstoned()) { // paranoid
                MiruHost host = hostMarshaller.fromBytes(key.getKey());
                long timestamp = FilerIO.bytesLong(value.getValue());
                heartbeats.add(new HostHeartbeat(host, timestamp));
            }
            return true;
        });
        return heartbeats;
    }

    private WALKey topologyKey(MiruTenantId tenantId, MiruPartitionId partitionId) {
        int length = tenantId.length();
        ByteBuffer bb = ByteBuffer.allocate(length + 4 + 4);
        bb.put(tenantId.getBytes());
        bb.putInt(partitionId.getId());
        bb.putInt(length);
        return new WALKey(bb.array());
    }

    private TenantAndPartition topologyKey(WALKey walKey) {
        ByteBuffer bb = ByteBuffer.wrap(walKey.getKey());
        bb.position(bb.limit() - 4);
        int length = bb.getInt();
        bb.position(0);
        byte[] tenantBytes = new byte[length];
        bb.get(tenantBytes);
        return new TenantAndPartition(tenantBytes, bb.getInt());
    }

    @Override
    public void changes(RowsChanged rowsChanged) throws Exception {
        if (rowsChanged.getRegionName().equals(RegionProvider.RING_INDEX)) {
            ringInitialized.set(false);
        }
    }

    static class TenantAndPartition {

        public final byte[] tenantBytes;
        public final int partitionId;

        public TenantAndPartition(byte[] miruTenantId, int miruPartitionId) {
            this.tenantBytes = miruTenantId;
            this.partitionId = miruPartitionId;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            TenantAndPartition that = (TenantAndPartition) o;

            if (partitionId != that.partitionId) {
                return false;
            }
            return Arrays.equals(tenantBytes, that.tenantBytes);

        }

        @Override
        public int hashCode() {
            int result = tenantBytes != null ? Arrays.hashCode(tenantBytes) : 0;
            result = 31 * result + partitionId;
            return result;
        }
    }

    private AmzaRegion createRegionIfAbsent(String regionName) throws Exception {
        if (!ringInitialized.get()) {
            List<RingHost> ring = amzaService.getAmzaRing().getRing(AMZA_RING_NAME);
            int desiredRingSize = amzaService.getAmzaRing().getRing("system").size();
            if (ring.size() < desiredRingSize) {
                amzaService.getAmzaRing().buildRandomSubRing(AMZA_RING_NAME, desiredRingSize);
            }
            ringInitialized.set(true);
        }

        return amzaService.createRegionIfAbsent(new RegionName(false, AMZA_RING_NAME, regionName),
            new RegionProperties(amzaStorageDescriptor, replicationFactor, takeFromFactor, false)
        );
    }

    @Override
    public void updateTopologies(MiruHost host, Collection<TopologyUpdate> topologyUpdates) throws Exception {

        final AmzaRegion topologyInfo = createRegionIfAbsent("host-" + host.toStringForm() + "-partition-info");

        ImmutableMap<WALKey, TopologyUpdate> keyToUpdateMap = Maps.uniqueIndex(topologyUpdates, topologyUpdate -> {
            return topologyKey(topologyUpdate.coord.tenantId, topologyUpdate.coord.partitionId);
        });

        Map<WALKey, byte[]> keyToBytesMap = Maps.transformValues(keyToUpdateMap, topologyUpdate -> {
            try {
                MiruPartitionCoord coord = topologyUpdate.coord;
                Optional<MiruPartitionCoordInfo> optionalInfo = topologyUpdate.optionalInfo;
                Optional<Long> refreshIngressTimestamp = topologyUpdate.ingressTimestamp;
                Optional<Long> refreshQueryTimestamp = topologyUpdate.queryTimestamp;
                WALKey key = topologyKey(coord.tenantId, coord.partitionId);

                MiruPartitionCoordInfo coordInfo;
                if (optionalInfo.isPresent()) {
                    coordInfo = optionalInfo.get();
                } else {
                    byte[] got = topologyInfo.get(key);
                    if (got != null) {
                        MiruTopologyColumnValue value = topologyColumnValueMarshaller.fromBytes(got);
                        coordInfo = new MiruPartitionCoordInfo(value.state, value.storage);
                    } else {
                        coordInfo = new MiruPartitionCoordInfo(MiruPartitionState.offline, MiruBackingStorage.memory);
                    }
                }
                long ingressTimestamp = refreshIngressTimestamp.or(-1L);
                long queryTimestamp = refreshQueryTimestamp.or(-1L);
                if (ingressTimestamp == -1 || queryTimestamp == -1) {
                    byte[] got = topologyInfo.get(key);
                    if (got != null) {
                        MiruTopologyColumnValue value = topologyColumnValueMarshaller.fromBytes(got);
                        ingressTimestamp = (ingressTimestamp == -1) ? value.lastIngressTimestamp : ingressTimestamp;
                        queryTimestamp = (queryTimestamp == -1) ? value.lastQueryTimestamp : queryTimestamp;
                    } else {
                        ingressTimestamp = (ingressTimestamp == -1) ? 0 : ingressTimestamp;
                        queryTimestamp = (queryTimestamp == -1) ? 0 : queryTimestamp;
                    }
                }

                MiruTopologyColumnValue value = new MiruTopologyColumnValue(coordInfo.state, coordInfo.storage, ingressTimestamp, queryTimestamp);
                LOG.debug("Updating {} to {} at ingress={} query={}", new Object[] { coord, coordInfo, ingressTimestamp, queryTimestamp });
                return topologyColumnValueMarshaller.toBytes(value);
            } catch (Exception e) {
                throw new RuntimeException("Failed to apply topology update", e);
            }
        });

        topologyInfo.set(keyToBytesMap.entrySet());

        markTenantTopologyUpdated(topologyUpdates.stream()
            .filter(input -> input.optionalInfo.isPresent())
            .map(input -> input.coord.tenantId)
            .collect(Collectors.toSet()));
    }

    private void markTenantTopologyUpdated(Set<MiruTenantId> tenantIds) throws Exception {
        Map<WALKey, byte[]> tenantUpdates = Maps.newHashMap();
        for (MiruTenantId tenantId : tenantIds) {
            tenantUpdates.put(new WALKey(tenantId.getBytes()), EMPTY_BYTES);
        }

        for (HostHeartbeat heartbeat : getAllHosts()) {
            AmzaRegion topology = createRegionIfAbsent("host-" + heartbeat.host.toStringForm() + "-topology-updates");
            topology.set(tenantUpdates.entrySet());
        }
    }

    @Override
    public NamedCursorsResult<Collection<MiruTenantTopologyUpdate>> getTopologyUpdatesForHost(MiruHost host,
        Collection<NamedCursor> sinceCursors)
        throws Exception {

        final Map<MiruTenantId, MiruTenantTopologyUpdate> updates = Maps.newHashMap();

        String ringHost = amzaService.getAmzaRing().getRingHost().toCanonicalString();
        long sinceTransactionId = 0;
        for (NamedCursor sinceCursor : sinceCursors) {
            if (ringHost.equals(sinceCursor.name)) {
                sinceTransactionId = sinceCursor.id;
                break;
            }
        }

        AmzaRegion topologyUpdates = createRegionIfAbsent("host-" + host.toStringForm() + "-topology-updates");
        TakeCursors takeCursors = amzaService.takeFromTransactionId(topologyUpdates, sinceTransactionId, (transactionId, key, value) -> {
            MiruTenantId tenantId = new MiruTenantId(key.getKey());
            long timestampId = value.getTimestampId();
            MiruTenantTopologyUpdate existing = updates.get(tenantId);
            if (existing == null || existing.timestamp < timestampId) {
                updates.put(tenantId, new MiruTenantTopologyUpdate(tenantId, timestampId));
            }
            return true;
        });

        Collection<NamedCursor> cursors = sinceCursors;
        if (takeCursors != null) {
            cursors = Lists.newArrayList();
            for (TakeCursors.RingHostCursor cursor : takeCursors.ringHostCursors) {
                cursors.add(new NamedCursor(cursor.ringHost.toCanonicalString(), cursor.transactionId));
            }
        }

        return new NamedCursorsResult<>(cursors, updates.values());
    }

    @Override
    public NamedCursorsResult<Collection<MiruPartitionActiveUpdate>> getPartitionActiveUpdatesForHost(MiruHost host, Collection<NamedCursor> sinceCursors)
        throws Exception {

        String partitionRegistryName = "host-" + host.toStringForm() + "-partition-registry";
        String partitionInfoName = "host-" + host.toStringForm() + "-partition-info";
        AmzaRegion partitionRegistry = createRegionIfAbsent(partitionRegistryName);
        AmzaRegion partitionInfo = createRegionIfAbsent(partitionInfoName);

        String ringHost = amzaService.getAmzaRing().getRingHost().toCanonicalString();
        String registryCursorName = ringHost + '/' + partitionRegistryName;
        String infoCursorName = ringHost + '/' + partitionInfoName;

        long registryTransactionId = 0;
        long infoTransactionId = 0;
        Map<String, NamedCursor> cursors = Maps.newHashMap();
        for (NamedCursor sinceCursor : sinceCursors) {
            cursors.put(sinceCursor.name, sinceCursor);
            if (registryCursorName.equals(sinceCursor.name)) {
                registryTransactionId = sinceCursor.id;
            } else if (infoCursorName.equals(sinceCursor.name)) {
                infoTransactionId = sinceCursor.id;
            }
        }

        final Set<TenantAndPartition> tenantPartitions = Sets.newHashSet();
        TakeCursors registryTakeCursors = amzaService.takeFromTransactionId(partitionRegistry, registryTransactionId, (transactionId, key, value) -> {
            TenantAndPartition tenantAndPartition = topologyKey(key);
            tenantPartitions.add(tenantAndPartition);
            return true;
        });
        TakeCursors infoTakeCursors = amzaService.takeFromTransactionId(partitionInfo, infoTransactionId, (transactionId, key, value) -> {
            TenantAndPartition tenantAndPartition = topologyKey(key);
            tenantPartitions.add(tenantAndPartition);
            return true;
        });

        List<MiruPartitionActiveUpdate> updates = Lists.newArrayList();
        ListMultimap<MiruTenantId, MiruPartitionId> tenantPartitionsMap = ArrayListMultimap.create();
        for (TenantAndPartition tenantPartition : tenantPartitions) {
            tenantPartitionsMap.put(new MiruTenantId(tenantPartition.tenantBytes), MiruPartitionId.of(tenantPartition.partitionId));
        }

        for (Map.Entry<MiruTenantId, Collection<MiruPartitionId>> entry : tenantPartitionsMap.asMap().entrySet()) {
            MiruTenantId tenantId = entry.getKey();
            Collection<MiruPartitionId> partitionIds = entry.getValue();
            NavigableMap<MiruPartitionId, MinMaxPriorityQueue<HostAndTimestamp>> replicaSets = tenantPartitionsLatestTopology(tenantId, partitionIds);
            for (MiruPartitionId partitionId : partitionIds) {
                boolean hosted = false;
                for (HostAndTimestamp hostAndTimestamp : replicaSets.get(partitionId)) {
                    if (hostAndTimestamp.host.equals(host)) {
                        hosted = true;
                        break;
                    }
                }
                MiruPartitionActive partitionActive = isPartitionActive(new MiruPartitionCoord(tenantId, partitionId, host));
                updates.add(new MiruPartitionActiveUpdate(tenantId, partitionId.getId(), hosted,
                    partitionActive.activeUntilTimestamp, partitionActive.idleAfterTimestamp, partitionActive.destroyAfterTimestamp));
            }
        }

        if (registryTakeCursors != null) {
            extractCursors(partitionRegistryName, cursors, registryTakeCursors);
        }
        if (infoTakeCursors != null) {
            extractCursors(partitionInfoName, cursors, infoTakeCursors);
        }

        return new NamedCursorsResult<>(cursors.values(), updates);
    }

    private void extractCursors(String partitionRegistryName, Map<String, NamedCursor> cursors, TakeCursors registryTakeCursors) {
        for (TakeCursors.RingHostCursor hostCursor : registryTakeCursors.ringHostCursors) {
            String name = hostCursor.ringHost.toCanonicalString() + '/' + partitionRegistryName;
            NamedCursor existing = cursors.get(name);
            if (existing == null || existing.id < hostCursor.transactionId) {
                cursors.put(name, new NamedCursor(name, hostCursor.transactionId));
            }
        }
    }

    @Override
    public void ensurePartitionCoord(MiruPartitionCoord coord) throws Exception {

        AmzaRegion topologyInfo = createRegionIfAbsent("host-" + coord.host.toStringForm() + "-partition-info");
        WALKey key = topologyKey(coord.tenantId, coord.partitionId);
        if (topologyInfo.get(key) == null) { // TODO don't have a set if absent. This is a little racy
            MiruTopologyColumnValue update = new MiruTopologyColumnValue(MiruPartitionState.offline, MiruBackingStorage.memory, 0, 0);
            topologyInfo.set(key, topologyColumnValueMarshaller.toBytes(update));
        }

    }

    @Override
    public void addToReplicaRegistry(MiruTenantId tenantId, MiruPartitionId partitionId, long nextId, MiruHost host) throws Exception {
        AmzaRegion topology = createRegionIfAbsent("host-" + host.toStringForm() + "-partition-registry");
        WALKey key = topologyKey(tenantId, partitionId);
        topology.set(key, FilerIO.longBytes(nextId)); // most recent is smallest.
        markTenantTopologyUpdated(Collections.singleton(tenantId));
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
        Set<MiruTenantId> tenants = getTenantsForHostAsSet(host);
        return new ArrayList<>(tenants);
    }

    private Set<MiruTenantId> getTenantsForHostAsSet(MiruHost host) throws Exception {
        final Set<MiruTenantId> tenants = new HashSet<>();
        AmzaRegion topology = createRegionIfAbsent("host-" + host.toStringForm() + "-partition-registry");
        topology.scan((transactionId, key, value) -> {
            TenantAndPartition topologyKey = topologyKey(key);
            tenants.add(new MiruTenantId(topologyKey.tenantBytes));
            return true;
        });
        return tenants;
    }

    @Override
    public void removeTenantPartionReplicaSet(final MiruTenantId tenantId, final MiruPartitionId partitionId) throws Exception {
        createRegionIfAbsent("hosts").scan((transactionId, key, value) -> {
            if (!value.getTombstoned()) { // paranoid
                MiruHost host = hostMarshaller.fromBytes(key.getKey());
                AmzaRegion topologyReg = createRegionIfAbsent("host-" + host.toStringForm() + "-partition-registry");
                AmzaRegion topologyInfo = createRegionIfAbsent("host-" + host.toStringForm() + "-partition-info");
                WALKey topologyKey = topologyKey(tenantId, partitionId);
                topologyReg.remove(topologyKey);
                topologyInfo.remove(topologyKey);
            }
            return true;
        });
        markTenantTopologyUpdated(Collections.singleton(tenantId));
    }

    @Override
    public List<MiruPartition> getPartitionsForTenant(MiruTenantId tenantId) throws Exception {
        NavigableMap<MiruPartitionId, MinMaxPriorityQueue<HostAndTimestamp>> paritionIdToLatest = tenantsLatestTopology(tenantId);

        List<MiruPartition> partitions = new ArrayList<>();
        for (MiruPartitionId partitionId : paritionIdToLatest.keySet()) {
            MinMaxPriorityQueue<HostAndTimestamp> got = paritionIdToLatest.get(partitionId);
            for (HostAndTimestamp hat : got) {
                AmzaRegion topologyInfo = createRegionIfAbsent("host-" + hat.host.toStringForm() + "-partition-info");
                byte[] rawInfo = topologyInfo.get(topologyKey(tenantId, partitionId));
                MiruPartitionCoordInfo info;
                if (rawInfo == null) {
                    info = new MiruPartitionCoordInfo(MiruPartitionState.offline, MiruBackingStorage.memory);
                } else {
                    MiruTopologyColumnValue columnValue = topologyColumnValueMarshaller.fromBytes(rawInfo);
                    info = new MiruPartitionCoordInfo(columnValue.state, columnValue.storage);
                }
                partitions.add(new MiruPartition(new MiruPartitionCoord(tenantId, partitionId, hat.host), info));
            }
        }
        return partitions;
    }

    private NavigableMap<MiruPartitionId, MinMaxPriorityQueue<HostAndTimestamp>> tenantsLatestTopology(MiruTenantId tenantId) throws Exception {
        final WALKey from = new WALKey(tenantId.getBytes());
        final WALKey to = new WALKey(prefixUpperExclusive(tenantId.getBytes()));
        final NavigableMap<MiruPartitionId, MinMaxPriorityQueue<HostAndTimestamp>> partitionIdToLatest = new TreeMap<>();
        createRegionIfAbsent("hosts").scan((hostsTransactionId, key, value) -> {
            if (!value.getTombstoned()) { // paranoid
                final MiruHost host = hostMarshaller.fromBytes(key.getKey());
                AmzaRegion topologyReg = createRegionIfAbsent("host-" + host.toStringForm() + "-partition-registry");
                topologyReg.rangeScan(from, to, (topologyTransactionId, key1, value1) -> {
                    TenantAndPartition topologyKey = topologyKey(key1);
                    MiruPartitionId partitionId = MiruPartitionId.of(topologyKey.partitionId);
                    MinMaxPriorityQueue<HostAndTimestamp> got = partitionIdToLatest.get(partitionId);
                    if (got == null) {
                        // TODO defaultNumberOfReplicas should come from config?
                        got = MinMaxPriorityQueue.maximumSize(defaultNumberOfReplicas)
                            .expectedSize(defaultNumberOfReplicas)
                            .create();
                        partitionIdToLatest.put(partitionId, got);
                    }
                    got.add(new HostAndTimestamp(host, FilerIO.bytesLong(value1.getValue())));
                    return true;
                });
            }
            return true;
        });
        return partitionIdToLatest;
    }

    private NavigableMap<MiruPartitionId, MinMaxPriorityQueue<HostAndTimestamp>> tenantPartitionsLatestTopology(final MiruTenantId tenantId,
        final Collection<MiruPartitionId> partitionIds) throws Exception {

        // TODO defaultNumberOfReplicas should come from config?
        final NavigableMap<MiruPartitionId, MinMaxPriorityQueue<HostAndTimestamp>> partitionIdToLatest = new TreeMap<>();
        for (MiruPartitionId partitionId : partitionIds) {
            partitionIdToLatest.put(partitionId,
                MinMaxPriorityQueue.maximumSize(defaultNumberOfReplicas)
                    .expectedSize(defaultNumberOfReplicas)
                    .<HostAndTimestamp>create());
        }

        createRegionIfAbsent("hosts").scan((hostsTransactionId, key, value) -> {
            if (!value.getTombstoned()) { // paranoid
                final MiruHost host = hostMarshaller.fromBytes(key.getKey());
                AmzaRegion topologyReg = createRegionIfAbsent("host-" + host.toStringForm() + "-partition-registry");
                for (final MiruPartitionId partitionId : partitionIds) {
                    topologyReg.get(Collections.singletonList(topologyKey(tenantId, partitionId)), (topologyTransactionId, key1, value1) -> {
                        partitionIdToLatest.get(partitionId).add(new HostAndTimestamp(host, FilerIO.bytesLong(value1.getValue())));
                        return true;
                    });
                }
            }
            return true;
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
        NavigableMap<MiruPartitionId, MinMaxPriorityQueue<HostAndTimestamp>> partitionIdToLatest = tenantsLatestTopology(tenantId);
        List<MiruPartition> partitions = new ArrayList<>();
        for (MiruPartitionId partitionId : partitionIdToLatest.keySet()) {
            MinMaxPriorityQueue<HostAndTimestamp> got = partitionIdToLatest.get(partitionId);
            for (HostAndTimestamp hat : got) {
                if (hat.host.equals(host)) {
                    AmzaRegion topologyInfo = createRegionIfAbsent("host-" + hat.host.toStringForm() + "-partition-info");
                    byte[] rawInfo = topologyInfo.get(topologyKey(tenantId, partitionId));
                    MiruPartitionCoordInfo info;
                    if (rawInfo == null) {
                        info = new MiruPartitionCoordInfo(MiruPartitionState.offline, MiruBackingStorage.memory);
                    } else {
                        MiruTopologyColumnValue columnValue = topologyColumnValueMarshaller.fromBytes(rawInfo);
                        info = new MiruPartitionCoordInfo(columnValue.state, columnValue.storage);
                    }
                    partitions.add(new MiruPartition(new MiruPartitionCoord(tenantId, partitionId, hat.host), info));
                }
            }
        }
        return partitions;
    }

    @Override
    public List<MiruTopologyStatus> getTopologyStatusForTenant(MiruTenantId tenantId) throws Exception {
        NavigableMap<MiruPartitionId, MinMaxPriorityQueue<HostAndTimestamp>> partitionIdToLatest = tenantsLatestTopology(tenantId);
        List<MiruTopologyStatus> status = new ArrayList<>();
        for (MiruPartitionId partitionId : partitionIdToLatest.keySet()) {
            MinMaxPriorityQueue<HostAndTimestamp> got = partitionIdToLatest.get(partitionId);
            for (HostAndTimestamp hat : got) {
                AmzaRegion topologyInfo = createRegionIfAbsent("host-" + hat.host.toStringForm() + "-partition-info");
                byte[] rawInfo = topologyInfo.get(topologyKey(tenantId, partitionId));
                MiruPartitionCoordInfo info;
                long lastIngressTimestampMillis = 0;
                long lastQueryTimestampMillis = 0;
                if (rawInfo == null) {
                    info = new MiruPartitionCoordInfo(MiruPartitionState.offline, MiruBackingStorage.memory);
                } else {
                    MiruTopologyColumnValue columnValue = topologyColumnValueMarshaller.fromBytes(rawInfo);
                    info = new MiruPartitionCoordInfo(columnValue.state, columnValue.storage);
                    lastIngressTimestampMillis = columnValue.lastIngressTimestamp;
                    lastQueryTimestampMillis = columnValue.lastQueryTimestamp;
                }
                MiruPartitionCoord coord = new MiruPartitionCoord(tenantId, partitionId, hat.host);
                MiruPartition miruPartition = new MiruPartition(coord, info);
                status.add(new MiruTopologyStatus(miruPartition, lastIngressTimestampMillis, lastQueryTimestampMillis,
                    getDestroyAfterTimestamp(coord, lastIngressTimestampMillis)));
            }
        }
        return status;
    }

    @Override
    public List<MiruTopologyStatus> getTopologyStatusForTenantHost(MiruTenantId tenantId, MiruHost host) throws Exception {
        NavigableMap<MiruPartitionId, MinMaxPriorityQueue<HostAndTimestamp>> partitionIdToLatest = tenantsLatestTopology(tenantId);
        List<MiruTopologyStatus> status = new ArrayList<>();
        for (MiruPartitionId partitionId : partitionIdToLatest.keySet()) {
            MinMaxPriorityQueue<HostAndTimestamp> got = partitionIdToLatest.get(partitionId);
            for (HostAndTimestamp hat : got) {
                if (hat.host.equals(host)) {
                    AmzaRegion topologyInfo = createRegionIfAbsent("host-" + hat.host.toStringForm() + "-partition-info");
                    byte[] rawInfo = topologyInfo.get(topologyKey(tenantId, partitionId));
                    MiruPartitionCoordInfo info;
                    long lastIngressTimestampMillis = 0;
                    long lastQueryTimestampMillis = 0;
                    if (rawInfo == null) {
                        info = new MiruPartitionCoordInfo(MiruPartitionState.offline, MiruBackingStorage.memory);
                    } else {
                        MiruTopologyColumnValue columnValue = topologyColumnValueMarshaller.fromBytes(rawInfo);
                        info = new MiruPartitionCoordInfo(columnValue.state, columnValue.storage);
                        lastIngressTimestampMillis = columnValue.lastIngressTimestamp;
                        lastQueryTimestampMillis = columnValue.lastQueryTimestamp;
                    }
                    MiruPartitionCoord coord = new MiruPartitionCoord(tenantId, partitionId, hat.host);
                    MiruPartition miruPartition = new MiruPartition(coord, info);
                    status.add(new MiruTopologyStatus(miruPartition, lastIngressTimestampMillis, lastQueryTimestampMillis,
                        getDestroyAfterTimestamp(coord, lastIngressTimestampMillis)));
                }
            }
        }
        return status;
    }

    @Override
    public Map<MiruPartitionId, MiruReplicaSet> getReplicaSets(MiruTenantId tenantId, Collection<MiruPartitionId> requiredPartitionId) throws Exception {
        NavigableMap<MiruPartitionId, MinMaxPriorityQueue<HostAndTimestamp>> partitionIdToLatest = tenantsLatestTopology(tenantId);
        ListMultimap<MiruPartitionId, MiruPartition> partitionsPartitions = ArrayListMultimap.create();
        SetMultimap<MiruPartitionId, MiruHost> partitionHosts = HashMultimap.create();

        for (MiruPartitionId partitionId : partitionIdToLatest.keySet()) {
            if (requiredPartitionId.contains(partitionId)) {
                MinMaxPriorityQueue<HostAndTimestamp> got = partitionIdToLatest.get(partitionId);
                for (HostAndTimestamp hat : got) {
                    AmzaRegion topologyInfo = createRegionIfAbsent("host-" + hat.host.toStringForm() + "-partition-info");
                    byte[] rawInfo = topologyInfo.get(topologyKey(tenantId, partitionId));
                    MiruPartitionCoordInfo info;
                    if (rawInfo == null) {
                        info = new MiruPartitionCoordInfo(MiruPartitionState.offline, MiruBackingStorage.memory);
                    } else {
                        MiruTopologyColumnValue columnValue = topologyColumnValueMarshaller.fromBytes(rawInfo);
                        info = new MiruPartitionCoordInfo(columnValue.state, columnValue.storage);
                    }
                    partitionsPartitions.put(partitionId, new MiruPartition(new MiruPartitionCoord(tenantId, partitionId, hat.host), info));
                    partitionHosts.put(partitionId, hat.host);
                }
            }
        }

        Map<MiruPartitionId, MiruReplicaSet> replicaSet = new HashMap<>();
        for (MiruPartitionId partitionId : requiredPartitionId) {
            List<MiruPartition> partitions = partitionsPartitions.get(partitionId);
            Set<MiruHost> replicaHosts = partitionHosts.get(partitionId);
            int missing = defaultNumberOfReplicas - replicaHosts.size(); // TODO expose to config?
            replicaSet.put(partitionId, new MiruReplicaSet(extractPartitionsByState(partitions), replicaHosts, missing));
        }
        return replicaSet;
    }

    private ListMultimap<MiruPartitionState, MiruPartition> extractPartitionsByState(List<MiruPartition> partitions) {
        return Multimaps.index(partitions, input -> input.info.state);
    }

    @Override
    public MiruPartitionActive isPartitionActive(MiruPartitionCoord coord) throws Exception {
        AmzaRegion topologyInfo = createRegionIfAbsent("host-" + coord.host.toStringForm() + "-partition-info");
        byte[] got = topologyInfo.get(topologyKey(coord.tenantId, coord.partitionId));
        if (got == null) {
            return new MiruPartitionActive(-1, -1, -1);
        } else {
            MiruTopologyColumnValue columnValue = topologyColumnValueMarshaller.fromBytes(got);
            long activeUntilTimestamp = -1;
            long idleAfterTimestamp = -1;
            long activeTimestamp = Math.max(columnValue.lastIngressTimestamp, columnValue.lastQueryTimestamp);
            if (activeTimestamp > 0) {
                activeUntilTimestamp = activeTimestamp + defaultTopologyIsStaleAfterMillis;
                idleAfterTimestamp = activeTimestamp + defaultTopologyIsIdleAfterMillis;
            }

            return new MiruPartitionActive(activeUntilTimestamp, idleAfterTimestamp, getDestroyAfterTimestamp(coord, columnValue.lastIngressTimestamp));
        }
    }

    private long getDestroyAfterTimestamp(MiruPartitionCoord coord, long lastIngressTimestamp) throws Exception {
        long destroyAfterTimestamp = -1;
        Optional<MiruWALClient.MiruLookupRange> range = rangeProvider.getRange(coord.tenantId, coord.partitionId, lastIngressTimestamp);
        if (range.isPresent() && range.get().maxClock > 0) {
            destroyAfterTimestamp = range.get().maxClock + defaultTopologyDestroyAfterMillis;
        }
        return destroyAfterTimestamp;
    }

    @Override
    public void removeHost(MiruHost host) throws Exception {
        Set<MiruTenantId> tenantIds = getTenantsForHostAsSet(host);

        // TODO add to Amza removeTable
        createRegionIfAbsent("hosts").remove(new WALKey(hostMarshaller.toBytes(host)));

        markTenantTopologyUpdated(tenantIds);
    }

    @Override
    public void removeTopology(MiruTenantId tenantId, MiruPartitionId partitionId, MiruHost host) throws Exception {
        AmzaRegion topologyInfo = createRegionIfAbsent("host-" + host.toStringForm() + "-partition-info");
        topologyInfo.remove(topologyKey(tenantId, partitionId));
        markTenantTopologyUpdated(Collections.singleton(tenantId));
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
        byte[] got = createRegionIfAbsent("schemas").get(new WALKey(tenantId.getBytes()));
        if (got == null) {
            return null;
        }
        return schemaMarshaller.fromBytes(got);
    }

    @Override
    public void registerSchema(MiruTenantId tenantId, MiruSchema schema) throws Exception {
        createRegionIfAbsent("schemas").set(new WALKey(tenantId.getBytes()), schemaMarshaller.toBytes(schema));
    }

}
