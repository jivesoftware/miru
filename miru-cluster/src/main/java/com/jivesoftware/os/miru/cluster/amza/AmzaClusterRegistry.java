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
import com.jivesoftware.os.miru.api.topology.MiruIngressUpdate;
import com.jivesoftware.os.miru.api.topology.MiruPartitionActive;
import com.jivesoftware.os.miru.api.topology.MiruPartitionActiveUpdate;
import com.jivesoftware.os.miru.api.topology.MiruTenantConfig;
import com.jivesoftware.os.miru.api.topology.MiruTenantTopologyUpdate;
import com.jivesoftware.os.miru.api.topology.NamedCursor;
import com.jivesoftware.os.miru.api.topology.NamedCursorsResult;
import com.jivesoftware.os.miru.api.topology.RangeMinMax;
import com.jivesoftware.os.miru.cluster.MiruClusterRegistry;
import com.jivesoftware.os.miru.cluster.MiruReplicaSet;
import com.jivesoftware.os.miru.cluster.MiruTenantConfigFields;
import com.jivesoftware.os.miru.cluster.marshaller.MiruHostMarshaller;
import com.jivesoftware.os.miru.cluster.marshaller.MiruTopologyColumnValueMarshaller;
import com.jivesoftware.os.miru.cluster.rcvs.MiruTopologyColumnValue;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import com.jivesoftware.os.rcvs.marshall.api.TypeMarshaller;
import com.jivesoftware.os.rcvs.marshall.api.UtilLexMarshaller;
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
    private static final String CLUSTER_REGISTRY_RING_NAME = "clusterRegistry";

    private static final String INGRESS_REGION_NAME = "partition-ingress";
    private static final String HOSTS_REGION_NAME = "hosts";
    private static final String SCHEMAS_REGION_NAME = "schemas";

    private static final String REGISTRY_SUFFIX = "partition-registry-v2";
    private static final String INFO_SUFFIX = "partition-info-v2";
    private static final String UPDATES_SUFFIX = "topology-updates-v2";

    private final MiruHostMarshaller hostMarshaller = new MiruHostMarshaller();
    private final MiruTopologyColumnValueMarshaller topologyColumnValueMarshaller = new MiruTopologyColumnValueMarshaller();
    private final TypeMarshaller<MiruSchema> schemaMarshaller;

    private final AmzaService amzaService;
    private final int defaultNumberOfReplicas;
    private final long defaultTopologyIsStaleAfterMillis;
    private final long defaultTopologyIsIdleAfterMillis;
    private final long defaultTopologyDestroyAfterMillis;
    private final int replicationFactor;
    private final int takeFromFactor;
    private final WALStorageDescriptor amzaStorageDescriptor;

    private final AtomicBoolean ringInitialized = new AtomicBoolean(false);

    public AmzaClusterRegistry(AmzaService amzaService,
        TypeMarshaller<MiruSchema> schemaMarshaller,
        int defaultNumberOfReplicas,
        long defaultTopologyIsStaleAfterMillis,
        long defaultTopologyIsIdleAfterMillis,
        long defaultTopologyDestroyAfterMillis,
        int replicationFactor,
        int takeFromFactor) throws Exception {
        this.amzaService = amzaService;
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

    private AmzaRegion createRegionIfAbsent(String regionName) throws Exception {
        amzaService.getAmzaRing().ensureMaximalSubRing(CLUSTER_REGISTRY_RING_NAME);
        return amzaService.createRegionIfAbsent(new RegionName(false, CLUSTER_REGISTRY_RING_NAME, regionName),
            new RegionProperties(amzaStorageDescriptor, replicationFactor, takeFromFactor, false));
    }

    private AmzaRegion getHostsRegion() throws Exception {
        return createRegionIfAbsent(HOSTS_REGION_NAME);
    }

    private AmzaRegion getIngressRegion() throws Exception {
        return createRegionIfAbsent(INGRESS_REGION_NAME);
    }

    private AmzaRegion getSchemasRegion() throws Exception {
        return createRegionIfAbsent(SCHEMAS_REGION_NAME);
    }

    private AmzaRegion getTopologyInfoRegion(MiruHost host) throws Exception {
        return createRegionIfAbsent("host-" + host.toStringForm() + "-" + INFO_SUFFIX);
    }

    private AmzaRegion getTopologyUpdatesRegion(MiruHost host) throws Exception {
        return createRegionIfAbsent("host-" + host.toStringForm() + "-" + UPDATES_SUFFIX);
    }

    private AmzaRegion getRegistryRegion(MiruHost host) throws Exception {
        return createRegionIfAbsent("host-" + host.toStringForm() + "-" + REGISTRY_SUFFIX);
    }

    private WALKey tenantKey(MiruTenantId tenantId) {
        int length = tenantId.length();
        ByteBuffer bb = ByteBuffer.allocate(4 + length);
        bb.putInt(length);
        bb.put(tenantId.getBytes());
        return new WALKey(bb.array());
    }

    private WALKey topologyKey(MiruTenantId tenantId, MiruPartitionId partitionId) {
        int length = tenantId.length();
        ByteBuffer bb = ByteBuffer.allocate(4 + length + 4);
        bb.putInt(length);
        bb.put(tenantId.getBytes());
        bb.putInt(partitionId.getId());
        return new WALKey(bb.array());
    }

    private WALKey topologyKeyPrefix(MiruTenantId tenantId) {
        int length = tenantId.length();
        ByteBuffer bb = ByteBuffer.allocate(4 + length);
        bb.putInt(length);
        bb.put(tenantId.getBytes());
        return new WALKey(bb.array());
    }

    private RawTenantAndPartition topologyKey(WALKey walKey) {
        ByteBuffer bb = ByteBuffer.wrap(walKey.getKey());
        int length = bb.getInt();
        byte[] tenantBytes = new byte[length];
        bb.get(tenantBytes);
        return new RawTenantAndPartition(tenantBytes, bb.getInt());
    }

    @Override
    public void changes(RowsChanged rowsChanged) throws Exception {
        if (rowsChanged.getRegionName().equals(RegionProvider.RING_INDEX)) {
            ringInitialized.set(false);
        }
    }

    @Override
    public void heartbeat(MiruHost miruHost) throws Exception {
        byte[] key = hostMarshaller.toBytes(miruHost);
        getHostsRegion().set(new WALKey(key), FilerIO.longBytes(System.currentTimeMillis()));
    }

    @Override
    public LinkedHashSet<HostHeartbeat> getAllHosts() throws Exception {
        final LinkedHashSet<HostHeartbeat> heartbeats = new LinkedHashSet<>();
        getHostsRegion().scan((transactionId, key, value) -> {
            if (!value.getTombstoned()) { // paranoid
                MiruHost host = hostMarshaller.fromBytes(key.getKey());
                long timestamp = FilerIO.bytesLong(value.getValue());
                heartbeats.add(new HostHeartbeat(host, timestamp));
            }
            return true;
        });
        return heartbeats;
    }

    @Override
    public void updateIngress(Collection<MiruIngressUpdate> ingressUpdates) throws Exception {
        AmzaRegion partitionIngress = getIngressRegion();
        Map<WALKey, byte[]> keyToBytesMap = Maps.newHashMap();
        for (MiruIngressUpdate ingressUpdate : ingressUpdates) {
            MiruTenantId tenantId = ingressUpdate.tenantId;
            MiruPartitionId partitionId = ingressUpdate.partitionId;

            IngressUpdate existing = ingressUpdate.absolute ? null : lookupIngress(tenantId, partitionId);

            if (ingressUpdate.ingressTimestamp != -1 && (existing == null || ingressUpdate.ingressTimestamp > existing.ingressTimestamp)) {
                keyToBytesMap.put(toIngressKey(tenantId, partitionId, IngressType.ingressTimestamp), FilerIO.longBytes(ingressUpdate.ingressTimestamp));
            }
            if (ingressUpdate.minMax.clockMin != -1 && (existing == null || ingressUpdate.minMax.clockMin < existing.clockMin)) {
                keyToBytesMap.put(toIngressKey(tenantId, partitionId, IngressType.clockMin), FilerIO.longBytes(ingressUpdate.minMax.clockMin));
            }
            if (ingressUpdate.minMax.clockMax != -1 && (existing == null || ingressUpdate.minMax.clockMax > existing.clockMax)) {
                keyToBytesMap.put(toIngressKey(tenantId, partitionId, IngressType.clockMax), FilerIO.longBytes(ingressUpdate.minMax.clockMax));
            }
            if (ingressUpdate.minMax.orderIdMin != -1 && (existing == null || ingressUpdate.minMax.orderIdMin < existing.orderIdMin)) {
                keyToBytesMap.put(toIngressKey(tenantId, partitionId, IngressType.orderIdMin), FilerIO.longBytes(ingressUpdate.minMax.orderIdMin));
            }
            if (ingressUpdate.minMax.orderIdMax != -1 && (existing == null || ingressUpdate.minMax.orderIdMax > existing.orderIdMax)) {
                keyToBytesMap.put(toIngressKey(tenantId, partitionId, IngressType.orderIdMax), FilerIO.longBytes(ingressUpdate.minMax.orderIdMax));
            }
        }
        partitionIngress.set(keyToBytesMap.entrySet());
    }

    @Override
    public void updateTopologies(MiruHost host, Collection<TopologyUpdate> topologyUpdates) throws Exception {
        final AmzaRegion topologyInfo = getTopologyInfoRegion(host);

        ImmutableMap<WALKey, TopologyUpdate> keyToUpdateMap = Maps.uniqueIndex(topologyUpdates, topologyUpdate -> {
            return topologyKey(topologyUpdate.coord.tenantId, topologyUpdate.coord.partitionId);
        });

        Map<WALKey, byte[]> keyToBytesMap = Maps.transformValues(keyToUpdateMap, topologyUpdate -> {
            try {
                MiruPartitionCoord coord = topologyUpdate.coord;
                Optional<MiruPartitionCoordInfo> optionalInfo = topologyUpdate.optionalInfo;
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
                long queryTimestamp = refreshQueryTimestamp.or(-1L);
                if (queryTimestamp == -1) {
                    byte[] got = topologyInfo.get(key);
                    if (got != null) {
                        MiruTopologyColumnValue value = topologyColumnValueMarshaller.fromBytes(got);
                        queryTimestamp = value.lastQueryTimestamp;
                    } else {
                        queryTimestamp = 0;
                    }
                }

                MiruTopologyColumnValue value = new MiruTopologyColumnValue(coordInfo.state, coordInfo.storage, queryTimestamp);
                LOG.debug("Updating {} to {} at query={}", new Object[] { coord, coordInfo, queryTimestamp });
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
            tenantUpdates.put(tenantKey(tenantId), EMPTY_BYTES);
        }

        for (HostHeartbeat heartbeat : getAllHosts()) {
            AmzaRegion topology = getTopologyUpdatesRegion(heartbeat.host);
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

        AmzaRegion topologyUpdates = getTopologyUpdatesRegion(host);
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

        AmzaRegion partitionRegistry = getRegistryRegion(host);
        AmzaRegion partitionInfo = getTopologyInfoRegion(host);
        AmzaRegion partitionIngress = getIngressRegion();

        String partitionRegistryName = "host-" + host.toStringForm() + "-" + REGISTRY_SUFFIX;
        String partitionInfoName = "host-" + host.toStringForm() + "-" + INFO_SUFFIX;
        String partitionIngressName = "host-" + host.toStringForm() + "-" + INGRESS_REGION_NAME; // global region still gets host prefix

        String ringHost = amzaService.getAmzaRing().getRingHost().toCanonicalString();
        String registryCursorName = ringHost + '/' + partitionRegistryName;
        String infoCursorName = ringHost + '/' + partitionInfoName;
        String ingressCursorName = ringHost + '/' + partitionIngressName;

        long registryTransactionId = 0;
        long infoTransactionId = 0;
        long ingressTransactionId = 0;
        Map<String, NamedCursor> cursors = Maps.newHashMap();
        for (NamedCursor sinceCursor : sinceCursors) {
            cursors.put(sinceCursor.name, sinceCursor);
            if (registryCursorName.equals(sinceCursor.name)) {
                registryTransactionId = sinceCursor.id;
            } else if (infoCursorName.equals(sinceCursor.name)) {
                infoTransactionId = sinceCursor.id;
            } else if (ingressCursorName.equals(sinceCursor.name)) {
                ingressTransactionId = sinceCursor.id;
            }
        }

        final Set<RawTenantAndPartition> tenantPartitions = Sets.newHashSet();
        TakeCursors registryTakeCursors = amzaService.takeFromTransactionId(partitionRegistry, registryTransactionId, (transactionId, key, value) -> {
            RawTenantAndPartition tenantAndPartition = topologyKey(key);
            tenantPartitions.add(tenantAndPartition);
            return true;
        });
        TakeCursors infoTakeCursors = amzaService.takeFromTransactionId(partitionInfo, infoTransactionId, (transactionId, key, value) -> {
            RawTenantAndPartition tenantAndPartition = topologyKey(key);
            tenantPartitions.add(tenantAndPartition);
            return true;
        });
        TakeCursors ingressTakeCursors = amzaService.takeFromTransactionId(partitionIngress, ingressTransactionId, (transactionId, key, value) -> {
            RawTenantAndPartition tenantAndPartition = topologyKey(key);
            tenantPartitions.add(tenantAndPartition);
            return true;
        });

        List<MiruPartitionActiveUpdate> updates = Lists.newArrayList();
        ListMultimap<MiruTenantId, MiruPartitionId> tenantPartitionsMap = ArrayListMultimap.create();
        for (RawTenantAndPartition tenantPartition : tenantPartitions) {
            tenantPartitionsMap.put(new MiruTenantId(tenantPartition.tenantBytes), MiruPartitionId.of(tenantPartition.partitionId));
        }

        for (Map.Entry<MiruTenantId, Collection<MiruPartitionId>> entry : tenantPartitionsMap.asMap().entrySet()) {
            MiruTenantId tenantId = entry.getKey();
            Collection<MiruPartitionId> partitionIds = entry.getValue();
            NavigableMap<MiruPartitionId, MinMaxPriorityQueue<HostAndTimestamp>> replicaSets = tenantPartitionsLatestTopologies(tenantId, partitionIds);
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
        if (ingressTakeCursors != null) {
            extractCursors(partitionIngressName, cursors, ingressTakeCursors);
        }

        return new NamedCursorsResult<>(cursors.values(), updates);
    }

    private void extractCursors(String cursorName, Map<String, NamedCursor> cursors, TakeCursors registryTakeCursors) {
        for (TakeCursors.RingHostCursor hostCursor : registryTakeCursors.ringHostCursors) {
            String name = hostCursor.ringHost.toCanonicalString() + '/' + cursorName;
            NamedCursor existing = cursors.get(name);
            if (existing == null || existing.id < hostCursor.transactionId) {
                cursors.put(name, new NamedCursor(name, hostCursor.transactionId));
            }
        }
    }

    @Override
    public void ensurePartitionCoord(MiruPartitionCoord coord) throws Exception {

        AmzaRegion topologyInfo = getTopologyInfoRegion(coord.host);
        WALKey key = topologyKey(coord.tenantId, coord.partitionId);
        if (topologyInfo.get(key) == null) { // TODO don't have a set if absent. This is a little racy
            MiruTopologyColumnValue update = new MiruTopologyColumnValue(MiruPartitionState.offline, MiruBackingStorage.memory, 0);
            topologyInfo.set(key, topologyColumnValueMarshaller.toBytes(update));
        }

    }

    @Override
    public void addToReplicaRegistry(MiruTenantId tenantId, MiruPartitionId partitionId, long nextId, MiruHost host) throws Exception {
        AmzaRegion topology = getRegistryRegion(host);
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
        AmzaRegion topology = getRegistryRegion(host);
        topology.scan((transactionId, key, value) -> {
            RawTenantAndPartition topologyKey = topologyKey(key);
            tenants.add(new MiruTenantId(topologyKey.tenantBytes));
            return true;
        });
        return tenants;
    }

    @Override
    public void removeTenantPartionReplicaSet(final MiruTenantId tenantId, final MiruPartitionId partitionId) throws Exception {
        getHostsRegion().scan((transactionId, key, value) -> {
            if (!value.getTombstoned()) { // paranoid
                MiruHost host = hostMarshaller.fromBytes(key.getKey());
                AmzaRegion topologyReg = getRegistryRegion(host);
                AmzaRegion topologyInfo = getTopologyInfoRegion(host);
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
        NavigableMap<MiruPartitionId, MinMaxPriorityQueue<HostAndTimestamp>> partitionIdToLatest = tenantLatestTopologies(tenantId);

        List<MiruPartition> partitions = new ArrayList<>();
        for (MiruPartitionId partitionId : partitionIdToLatest.keySet()) {
            MinMaxPriorityQueue<HostAndTimestamp> got = partitionIdToLatest.get(partitionId);
            for (HostAndTimestamp hat : got) {
                AmzaRegion topologyInfo = getTopologyInfoRegion(hat.host);
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

    private MinMaxPriorityQueue<HostAndTimestamp> tenantLatestTopology(MiruTenantId tenantId, MiruPartitionId partitionId) throws Exception {
        // TODO defaultNumberOfReplicas should come from config?
        MinMaxPriorityQueue<HostAndTimestamp> latest = MinMaxPriorityQueue.maximumSize(defaultNumberOfReplicas)
            .expectedSize(defaultNumberOfReplicas)
            .create();
        for (HostHeartbeat hostHeartbeat : getAllHosts()) {
            AmzaRegion registryRegion = getRegistryRegion(hostHeartbeat.host);
            byte[] got = registryRegion.get(topologyKey(tenantId, partitionId));
            if (got != null) {
                latest.add(new HostAndTimestamp(hostHeartbeat.host, FilerIO.bytesLong(got)));
            }
        }
        return latest;
    }

    private NavigableMap<MiruPartitionId, MinMaxPriorityQueue<HostAndTimestamp>> tenantLatestTopologies(MiruTenantId tenantId) throws Exception {
        final WALKey from = topologyKeyPrefix(tenantId);
        final WALKey to = from.prefixUpperExclusive();
        final NavigableMap<MiruPartitionId, MinMaxPriorityQueue<HostAndTimestamp>> partitionIdToLatest = new TreeMap<>();
        for (HostHeartbeat hostHeartbeat : getAllHosts()) {
            AmzaRegion registryRegion = getRegistryRegion(hostHeartbeat.host);
            registryRegion.rangeScan(from, to, (topologyTransactionId, topologyKey, topologyValue) -> {
                RawTenantAndPartition tenantPartitionKey = topologyKey(topologyKey);
                MiruPartitionId partitionId = MiruPartitionId.of(tenantPartitionKey.partitionId);
                MinMaxPriorityQueue<HostAndTimestamp> latest = partitionIdToLatest.get(partitionId);
                if (latest == null) {
                    // TODO defaultNumberOfReplicas should come from config?
                    latest = MinMaxPriorityQueue.maximumSize(defaultNumberOfReplicas)
                        .expectedSize(defaultNumberOfReplicas)
                        .create();
                    partitionIdToLatest.put(partitionId, latest);
                }
                latest.add(new HostAndTimestamp(hostHeartbeat.host, FilerIO.bytesLong(topologyValue.getValue())));
                return true;
            });
        }
        return partitionIdToLatest;
    }

    private NavigableMap<MiruPartitionId, MinMaxPriorityQueue<HostAndTimestamp>> tenantPartitionsLatestTopologies(MiruTenantId tenantId,
        Collection<MiruPartitionId> partitionIds) throws Exception {

        final NavigableMap<MiruPartitionId, MinMaxPriorityQueue<HostAndTimestamp>> partitionIdToLatest = new TreeMap<>();

        for (HostHeartbeat hostHeartbeat : getAllHosts()) {
            AmzaRegion registryRegion = getRegistryRegion(hostHeartbeat.host);
            for (MiruPartitionId partitionId : partitionIds) {
                byte[] got = registryRegion.get(topologyKey(tenantId, partitionId));
                if (got != null) {
                    MinMaxPriorityQueue<HostAndTimestamp> latest = partitionIdToLatest.get(partitionId);
                    if (latest == null) {
                        // TODO defaultNumberOfReplicas should come from config?
                        latest = MinMaxPriorityQueue.maximumSize(defaultNumberOfReplicas)
                            .expectedSize(defaultNumberOfReplicas)
                            .<HostAndTimestamp>create();
                        partitionIdToLatest.put(partitionId, latest);
                    }
                    latest.add(new HostAndTimestamp(hostHeartbeat.host, FilerIO.bytesLong(got)));
                }
            }
        }

        return partitionIdToLatest;
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
        NavigableMap<MiruPartitionId, MinMaxPriorityQueue<HostAndTimestamp>> partitionIdToLatest = tenantLatestTopologies(tenantId);
        List<MiruPartition> partitions = new ArrayList<>();
        for (MiruPartitionId partitionId : partitionIdToLatest.keySet()) {
            MinMaxPriorityQueue<HostAndTimestamp> got = partitionIdToLatest.get(partitionId);
            for (HostAndTimestamp hat : got) {
                if (hat.host.equals(host)) {
                    AmzaRegion topologyInfo = getTopologyInfoRegion(hat.host);
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
        NavigableMap<MiruPartitionId, MinMaxPriorityQueue<HostAndTimestamp>> partitionIdToLatest = tenantLatestTopologies(tenantId);
        List<MiruTopologyStatus> status = new ArrayList<>();
        for (MiruPartitionId partitionId : partitionIdToLatest.keySet()) {
            MinMaxPriorityQueue<HostAndTimestamp> got = partitionIdToLatest.get(partitionId);
            for (HostAndTimestamp hat : got) {
                AmzaRegion topologyInfo = getTopologyInfoRegion(hat.host);
                byte[] rawInfo = topologyInfo.get(topologyKey(tenantId, partitionId));
                MiruPartitionCoordInfo info;
                long lastIngressTimestampMillis = 0;
                long lastQueryTimestampMillis = 0;
                if (rawInfo == null) {
                    info = new MiruPartitionCoordInfo(MiruPartitionState.offline, MiruBackingStorage.memory);
                } else {
                    MiruTopologyColumnValue columnValue = topologyColumnValueMarshaller.fromBytes(rawInfo);
                    info = new MiruPartitionCoordInfo(columnValue.state, columnValue.storage);
                    lastQueryTimestampMillis = columnValue.lastQueryTimestamp;
                }
                MiruPartitionCoord coord = new MiruPartitionCoord(tenantId, partitionId, hat.host);
                MiruPartition miruPartition = new MiruPartition(coord, info);
                status.add(new MiruTopologyStatus(miruPartition, lastIngressTimestampMillis, lastQueryTimestampMillis, getDestroyAfterTimestamp(coord)));
            }
        }
        return status;
    }

    @Override
    public List<MiruTopologyStatus> getTopologyStatusForTenantHost(MiruTenantId tenantId, MiruHost host) throws Exception {
        NavigableMap<MiruPartitionId, MinMaxPriorityQueue<HostAndTimestamp>> partitionIdToLatest = tenantLatestTopologies(tenantId);
        List<MiruTopologyStatus> status = new ArrayList<>();
        for (MiruPartitionId partitionId : partitionIdToLatest.keySet()) {
            MinMaxPriorityQueue<HostAndTimestamp> got = partitionIdToLatest.get(partitionId);
            for (HostAndTimestamp hat : got) {
                if (hat.host.equals(host)) {
                    AmzaRegion topologyInfo = getTopologyInfoRegion(hat.host);
                    byte[] rawInfo = topologyInfo.get(topologyKey(tenantId, partitionId));
                    MiruPartitionCoordInfo info;
                    long lastIngressTimestampMillis = 0;
                    long lastQueryTimestampMillis = 0;
                    if (rawInfo == null) {
                        info = new MiruPartitionCoordInfo(MiruPartitionState.offline, MiruBackingStorage.memory);
                    } else {
                        MiruTopologyColumnValue columnValue = topologyColumnValueMarshaller.fromBytes(rawInfo);
                        info = new MiruPartitionCoordInfo(columnValue.state, columnValue.storage);
                        lastQueryTimestampMillis = columnValue.lastQueryTimestamp;
                    }
                    MiruPartitionCoord coord = new MiruPartitionCoord(tenantId, partitionId, hat.host);
                    MiruPartition miruPartition = new MiruPartition(coord, info);
                    status.add(new MiruTopologyStatus(miruPartition, lastIngressTimestampMillis, lastQueryTimestampMillis, getDestroyAfterTimestamp(coord)));
                }
            }
        }
        return status;
    }

    @Override
    public MiruReplicaSet getReplicaSet(MiruTenantId tenantId, MiruPartitionId partitionId) throws Exception {
        MinMaxPriorityQueue<HostAndTimestamp> latest = tenantLatestTopology(tenantId, partitionId);
        List<MiruPartition> partitions = Lists.newArrayList();
        Set<MiruHost> replicaHosts = Sets.newHashSet();

        for (HostAndTimestamp hat : latest) {
            AmzaRegion topologyInfo = getTopologyInfoRegion(hat.host);
            byte[] rawInfo = topologyInfo.get(topologyKey(tenantId, partitionId));
            MiruPartitionCoordInfo info;
            if (rawInfo == null) {
                info = new MiruPartitionCoordInfo(MiruPartitionState.offline, MiruBackingStorage.memory);
            } else {
                MiruTopologyColumnValue columnValue = topologyColumnValueMarshaller.fromBytes(rawInfo);
                info = new MiruPartitionCoordInfo(columnValue.state, columnValue.storage);
            }
            partitions.add(new MiruPartition(new MiruPartitionCoord(tenantId, partitionId, hat.host), info));
            replicaHosts.add(hat.host);
        }

        int missing = defaultNumberOfReplicas - replicaHosts.size(); // TODO expose to config?
        return new MiruReplicaSet(extractPartitionsByState(partitions), replicaHosts, missing);
    }

    @Override
    public Map<MiruPartitionId, MiruReplicaSet> getReplicaSets(MiruTenantId tenantId, Collection<MiruPartitionId> requiredPartitionId) throws Exception {
        NavigableMap<MiruPartitionId, MinMaxPriorityQueue<HostAndTimestamp>> partitionIdToLatest = tenantLatestTopologies(tenantId);
        ListMultimap<MiruPartitionId, MiruPartition> partitionsPartitions = ArrayListMultimap.create();
        SetMultimap<MiruPartitionId, MiruHost> partitionHosts = HashMultimap.create();

        for (MiruPartitionId partitionId : partitionIdToLatest.keySet()) {
            if (requiredPartitionId.contains(partitionId)) {
                MinMaxPriorityQueue<HostAndTimestamp> got = partitionIdToLatest.get(partitionId);
                for (HostAndTimestamp hat : got) {
                    AmzaRegion topologyInfo = getTopologyInfoRegion(hat.host);
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

        Map<MiruPartitionId, MiruReplicaSet> replicaSets = new HashMap<>();
        for (MiruPartitionId partitionId : requiredPartitionId) {
            List<MiruPartition> partitions = partitionsPartitions.get(partitionId);
            Set<MiruHost> replicaHosts = partitionHosts.get(partitionId);
            int missing = defaultNumberOfReplicas - replicaHosts.size(); // TODO expose to config?
            replicaSets.put(partitionId, new MiruReplicaSet(extractPartitionsByState(partitions), replicaHosts, missing));
        }
        return replicaSets;
    }

    private ListMultimap<MiruPartitionState, MiruPartition> extractPartitionsByState(List<MiruPartition> partitions) {
        return Multimaps.index(partitions, input -> input.info.state);
    }

    private MiruPartitionActive isPartitionActive(MiruPartitionCoord coord) throws Exception {
        long lastIngressTimestamp = getIngressUpdate(coord, IngressType.ingressTimestamp, -1L);
        long lastQueryTimestamp = -1;

        WALKey key = topologyKey(coord.tenantId, coord.partitionId);
        AmzaRegion topologyInfo = getTopologyInfoRegion(coord.host);
        byte[] gotTopology = topologyInfo.get(key);
        if (gotTopology != null) {
            MiruTopologyColumnValue topologyValue = topologyColumnValueMarshaller.fromBytes(gotTopology);
            lastQueryTimestamp = topologyValue.lastQueryTimestamp;
        }

        long activeUntilTimestamp = -1;
        long idleAfterTimestamp = -1;
        long activeTimestamp = Math.max(lastIngressTimestamp, lastQueryTimestamp);
        if (activeTimestamp > 0) {
            activeUntilTimestamp = activeTimestamp + defaultTopologyIsStaleAfterMillis;
            idleAfterTimestamp = activeTimestamp + defaultTopologyIsIdleAfterMillis;
        }

        return new MiruPartitionActive(activeUntilTimestamp, idleAfterTimestamp, getDestroyAfterTimestamp(coord));
    }

    private long getDestroyAfterTimestamp(MiruPartitionCoord coord) throws Exception {
        long destroyAfterTimestamp = -1;
        long clockMax = getIngressUpdate(coord, IngressType.clockMax, -1);
        if (clockMax > 0) {
            destroyAfterTimestamp = clockMax + defaultTopologyDestroyAfterMillis;
        }
        return destroyAfterTimestamp;
    }

    @Override
    public void removeHost(MiruHost host) throws Exception {
        Set<MiruTenantId> tenantIds = getTenantsForHostAsSet(host);

        // TODO add to Amza removeTable
        getHostsRegion().remove(new WALKey(hostMarshaller.toBytes(host)));

        markTenantTopologyUpdated(tenantIds);
    }

    @Override
    public void removeTopology(MiruTenantId tenantId, MiruPartitionId partitionId, MiruHost host) throws Exception {
        AmzaRegion topologyInfo = getTopologyInfoRegion(host);
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
        byte[] got = getSchemasRegion().get(tenantKey(tenantId));
        if (got == null) {
            return null;
        }
        return schemaMarshaller.fromBytes(got);
    }

    @Override
    public void registerSchema(MiruTenantId tenantId, MiruSchema schema) throws Exception {
        getSchemasRegion().set(tenantKey(tenantId), schemaMarshaller.toBytes(schema));
    }

    @Override
    public Map<MiruPartitionId, RangeMinMax> getIngressRanges(MiruTenantId tenantId) throws Exception {
        final Map<MiruPartitionId, RangeMinMax> partitionLookupRange = Maps.newHashMap();
        streamRanges(tenantId, null, (partitionId, type, timestamp) -> {
            RangeMinMax lookupRange = partitionLookupRange.get(partitionId);
            if (lookupRange == null) {
                lookupRange = new RangeMinMax();
                partitionLookupRange.put(partitionId, lookupRange);
            }
            if (type == IngressType.clockMin) {
                lookupRange.clockMin = timestamp;
            } else if (type == IngressType.clockMax) {
                lookupRange.clockMax = timestamp;
            } else if (type == IngressType.orderIdMin) {
                lookupRange.orderIdMin = timestamp;
            } else if (type == IngressType.orderIdMax) {
                lookupRange.orderIdMax = timestamp;
            }
            return true;
        });
        return partitionLookupRange;
    }

    private long getIngressUpdate(MiruPartitionCoord coord, IngressType type, long defaultValue) throws Exception {
        long value = defaultValue;
        AmzaRegion region = getIngressRegion();
        if (region != null) {
            byte[] clockMaxBytes = region.get(toIngressKey(coord.tenantId, coord.partitionId, type));
            if (clockMaxBytes != null) {
                value = FilerIO.bytesLong(clockMaxBytes);
            }
        }
        return value;
    }

    private IngressUpdate lookupIngress(MiruTenantId tenantId, MiruPartitionId partitionId) throws Exception {
        final IngressUpdate ingressUpdate = new IngressUpdate(partitionId.getId(), -1, -1, -1, -1); // ugly
        streamRanges(tenantId, partitionId, (streamPartitionId, type, timestamp) -> {
            if (partitionId.equals(streamPartitionId)) {
                if (type == IngressType.clockMin) {
                    ingressUpdate.clockMin = timestamp;
                } else if (type == IngressType.clockMax) {
                    ingressUpdate.clockMax = timestamp;
                } else if (type == IngressType.orderIdMin) {
                    ingressUpdate.orderIdMin = timestamp;
                } else if (type == IngressType.orderIdMax) {
                    ingressUpdate.orderIdMax = timestamp;
                }
                return true;
            } else {
                return false;
            }
        });
        return ingressUpdate;
    }

    private void streamRanges(MiruTenantId tenantId, MiruPartitionId partitionId, StreamRangeLookup streamRangeLookup) throws Exception {
        WALKey fromKey = toIngressKey(tenantId, partitionId, null);
        WALKey toKey = fromKey.prefixUpperExclusive();
        AmzaRegion region = getIngressRegion();
        if (region != null) {
            region.rangeScan(fromKey, toKey, (rowTxId, key, value) -> {
                if (value != null) {
                    MiruPartitionId streamPartitionId = ingressKeyToPartitionId(key.getKey());
                    if (partitionId == null || partitionId.equals(streamPartitionId)) {
                        IngressType ingressType = ingressKeyToType(key.getKey());
                        if (streamRangeLookup.stream(streamPartitionId, ingressType, value.getTimestampId())) {
                            return true;
                        }
                    }
                }
                return false;
            });
        }
    }

    private WALKey toIngressKey(MiruTenantId tenantId, MiruPartitionId partitionId, IngressType ingressType) {
        byte[] keyBytes;
        byte[] tenantBytes = tenantId.getBytes();
        if (partitionId != null) {
            if (ingressType != null) {
                keyBytes = new byte[4 + tenantBytes.length + 4 + 1];
                FilerIO.intBytes(tenantBytes.length, keyBytes, 0);
                System.arraycopy(tenantBytes, 0, keyBytes, 4, tenantBytes.length);
                UtilLexMarshaller.intBytes(partitionId.getId(), keyBytes, 4 + tenantBytes.length);
                keyBytes[keyBytes.length - 1] = ingressType.getType();
            } else {
                keyBytes = new byte[4 + tenantBytes.length + 4];
                FilerIO.intBytes(tenantBytes.length, keyBytes, 0);
                System.arraycopy(tenantBytes, 0, keyBytes, 4, tenantBytes.length);
                UtilLexMarshaller.intBytes(partitionId.getId(), keyBytes, 4 + tenantBytes.length);
            }
        } else {
            keyBytes = new byte[4 + tenantBytes.length];
            FilerIO.intBytes(tenantBytes.length, keyBytes, 0);
            System.arraycopy(tenantBytes, 0, keyBytes, 4, tenantBytes.length);
        }
        return new WALKey(keyBytes);
    }

    private MiruPartitionId ingressKeyToPartitionId(byte[] keyBytes) {
        return MiruPartitionId.of(FilerIO.bytesInt(keyBytes, keyBytes.length - 4 - 1));
    }

    private IngressType ingressKeyToType(byte[] keyBytes) {
        return IngressType.fromType(keyBytes[keyBytes.length - 1]);
    }

    private enum IngressType {

        ingressTimestamp((byte) 0),
        orderIdMin((byte) 1),
        orderIdMax((byte) 2),
        clockMin((byte) 3),
        clockMax((byte) 4);

        private final byte type;

        IngressType(byte type) {
            this.type = type;
        }

        public byte getType() {
            return type;
        }

        public static IngressType fromType(byte type) {
            for (IngressType ingressType : values()) {
                if (ingressType.type == type) {
                    return ingressType;
                }
            }
            return null;
        }
    }

    private interface StreamRangeLookup {

        boolean stream(MiruPartitionId partitionId, IngressType type, long timestamp);
    }

    private static class IngressUpdate {

        private long ingressTimestamp;
        private long orderIdMin;
        private long orderIdMax;
        private long clockMin;
        private long clockMax;

        private IngressUpdate(long ingressTimestamp, long orderIdMin, long orderIdMax, long clockMin, long clockMax) {
            this.ingressTimestamp = ingressTimestamp;
            this.orderIdMin = orderIdMin;
            this.orderIdMax = orderIdMax;
            this.clockMin = clockMin;
            this.clockMax = clockMax;
        }
    }

    private static class RawTenantAndPartition {

        private final byte[] tenantBytes;
        private final int partitionId;

        private RawTenantAndPartition(byte[] miruTenantId, int miruPartitionId) {
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

            RawTenantAndPartition that = (RawTenantAndPartition) o;

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
}
