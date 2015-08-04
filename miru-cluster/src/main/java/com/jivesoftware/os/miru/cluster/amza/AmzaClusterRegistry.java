package com.jivesoftware.os.miru.cluster.amza;

import com.google.common.base.Charsets;
import com.google.common.base.Optional;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.MinMaxPriorityQueue;
import com.google.common.collect.Multimaps;
import com.google.common.collect.SetMultimap;
import com.google.common.collect.Sets;
import com.jivesoftware.os.amza.client.AmzaKretrProvider;
import com.jivesoftware.os.amza.client.AmzaKretrProvider.AmzaKretr;
import com.jivesoftware.os.amza.service.AmzaService;
import com.jivesoftware.os.amza.service.storage.PartitionProvider;
import com.jivesoftware.os.amza.shared.AmzaPartitionUpdates;
import com.jivesoftware.os.amza.shared.partition.PartitionName;
import com.jivesoftware.os.amza.shared.partition.PartitionProperties;
import com.jivesoftware.os.amza.shared.partition.PrimaryIndexDescriptor;
import com.jivesoftware.os.amza.shared.scan.RowChanges;
import com.jivesoftware.os.amza.shared.scan.RowsChanged;
import com.jivesoftware.os.amza.shared.take.TakeCursors;
import com.jivesoftware.os.amza.shared.wal.WALKey;
import com.jivesoftware.os.amza.shared.wal.WALStorageDescriptor;
import com.jivesoftware.os.filer.io.FilerIO;
import com.jivesoftware.os.jive.utils.base.interfaces.CallbackStream;
import com.jivesoftware.os.miru.api.MiruBackingStorage;
import com.jivesoftware.os.miru.api.MiruHost;
import com.jivesoftware.os.miru.api.MiruPartition;
import com.jivesoftware.os.miru.api.MiruPartitionCoord;
import com.jivesoftware.os.miru.api.MiruPartitionCoordInfo;
import com.jivesoftware.os.miru.api.MiruPartitionState;
import com.jivesoftware.os.miru.api.activity.MiruPartitionId;
import com.jivesoftware.os.miru.api.activity.TenantAndPartition;
import com.jivesoftware.os.miru.api.activity.schema.MiruSchema;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.api.topology.HostHeartbeat;
import com.jivesoftware.os.miru.api.topology.MiruIngressUpdate;
import com.jivesoftware.os.miru.api.topology.MiruPartitionActive;
import com.jivesoftware.os.miru.api.topology.MiruPartitionActiveUpdate;
import com.jivesoftware.os.miru.api.topology.MiruPartitionStatus;
import com.jivesoftware.os.miru.api.topology.MiruTenantConfig;
import com.jivesoftware.os.miru.api.topology.MiruTenantTopologyUpdate;
import com.jivesoftware.os.miru.api.topology.MiruTopologyStatus;
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
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

/**
 * @author jonathan.colt
 */
public class AmzaClusterRegistry implements MiruClusterRegistry, RowChanges {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    private static final byte[] EMPTY_BYTES = new byte[0];
    private static final byte[] CLUSTER_REGISTRY_RING_NAME = "clusterRegistry".getBytes(Charsets.UTF_8);

    private static final String INGRESS_PARTITION_NAME = "partition-ingress";
    private static final String HOSTS_PARTITION_NAME = "hosts";
    private static final String SCHEMAS_PARTITION_NAME = "schemas";

    private static final String REGISTRY_SUFFIX = "partition-registry-v2";
    private static final String INFO_SUFFIX = "partition-info-v2";
    private static final String UPDATES_SUFFIX = "topology-updates-v2";

    private final MiruHostMarshaller hostMarshaller = new MiruHostMarshaller();
    private final MiruTopologyColumnValueMarshaller topologyColumnValueMarshaller = new MiruTopologyColumnValueMarshaller();
    private final TypeMarshaller<MiruSchema> schemaMarshaller;

    private final byte[] initialEnsuredTopologyBytes = topologyColumnValueMarshaller.toBytes(
        new MiruTopologyColumnValue(MiruPartitionState.offline, MiruBackingStorage.memory, 0));

    private final AmzaService amzaService;
    private final AmzaKretrProvider amzaKretrProvider;
    private final int replicateTakeQuorum;
    private final long replicateTimeoutMillis;
    private final int defaultNumberOfReplicas;
    private final long defaultTopologyIsStaleAfterMillis;
    private final long defaultTopologyIsIdleAfterMillis;
    private final long defaultTopologyDestroyAfterMillis;
    private final int takeFromFactor;
    private final WALStorageDescriptor amzaStorageDescriptor;

    private final AtomicBoolean ringInitialized = new AtomicBoolean(false);
    private final ConcurrentMap<String, AmzaKretr> clientMap = Maps.newConcurrentMap();

    public AmzaClusterRegistry(AmzaService amzaService,
        AmzaKretrProvider amzaKretrProvider,
        int replicateTakeQuorum,
        long replicateTimeoutMillis,
        TypeMarshaller<MiruSchema> schemaMarshaller,
        int defaultNumberOfReplicas,
        long defaultTopologyIsStaleAfterMillis,
        long defaultTopologyIsIdleAfterMillis,
        long defaultTopologyDestroyAfterMillis,
        int takeFromFactor) throws Exception {
        this.amzaService = amzaService;
        this.amzaKretrProvider = amzaKretrProvider;
        this.replicateTakeQuorum = replicateTakeQuorum;
        this.replicateTimeoutMillis = replicateTimeoutMillis;
        this.schemaMarshaller = schemaMarshaller;
        this.defaultNumberOfReplicas = defaultNumberOfReplicas;
        this.defaultTopologyIsStaleAfterMillis = defaultTopologyIsStaleAfterMillis;
        this.defaultTopologyIsIdleAfterMillis = defaultTopologyIsIdleAfterMillis;
        this.defaultTopologyDestroyAfterMillis = defaultTopologyDestroyAfterMillis;
        this.takeFromFactor = takeFromFactor;
        this.amzaStorageDescriptor = new WALStorageDescriptor(
            new PrimaryIndexDescriptor("berkeleydb", 0, false, null),
            null, 1000, 1000); //TODO config
    }

    private AmzaKretr ensureClient(String name) throws Exception {
        return clientMap.computeIfAbsent(name, s -> {
            try {
                amzaService.getRingWriter().ensureMaximalSubRing(CLUSTER_REGISTRY_RING_NAME);
                PartitionName partitionName = new PartitionName(false, CLUSTER_REGISTRY_RING_NAME, name.getBytes(Charsets.UTF_8));
                amzaService.setPropertiesIfAbsent(partitionName, new PartitionProperties(amzaStorageDescriptor, takeFromFactor, false));
                amzaService.awaitOnline(partitionName, 10_000L); //TODO config
                return amzaKretrProvider.getClient(partitionName);
            } catch (Exception e) {
                throw new RuntimeException("Failed to get amza client", e);
            }
        });
    }

    private AmzaKretr hostsClient() throws Exception {
        return ensureClient(HOSTS_PARTITION_NAME);
    }

    private AmzaKretr ingressClient() throws Exception {
        return ensureClient(INGRESS_PARTITION_NAME);
    }

    private AmzaKretr schemasClient() throws Exception {
        return ensureClient(SCHEMAS_PARTITION_NAME);
    }

    private AmzaKretr topologyInfoClient(MiruHost host) throws Exception {
        return ensureClient("host-" + host.toStringForm() + "-" + INFO_SUFFIX);
    }

    private AmzaKretr topologyUpdatesClient(MiruHost host) throws Exception {
        return ensureClient("host-" + host.toStringForm() + "-" + UPDATES_SUFFIX);
    }

    private AmzaKretr registryClient(MiruHost host) throws Exception {
        return ensureClient("host-" + host.toStringForm() + "-" + REGISTRY_SUFFIX);
    }

    private byte[] toTenantKey(MiruTenantId tenantId) {
        return tenantId.getBytes();
    }

    private MiruTenantId fromTenantKey(byte[] walKey) {
        return new MiruTenantId(walKey);
    }

    private byte[] toTopologyKey(MiruTenantId tenantId, MiruPartitionId partitionId) {
        int length = tenantId.length();
        ByteBuffer bb = ByteBuffer.allocate(4 + length + 4);
        bb.putInt(length);
        bb.put(tenantId.getBytes());
        bb.putInt(partitionId.getId());
        return bb.array();
    }

    private byte[] topologyKeyPrefix(MiruTenantId tenantId) {
        int length = tenantId.length();
        ByteBuffer bb = ByteBuffer.allocate(4 + length);
        bb.putInt(length);
        bb.put(tenantId.getBytes());
        return bb.array();
    }

    private RawTenantAndPartition fromTopologyKey(byte[] walKey) {
        ByteBuffer bb = ByteBuffer.wrap(walKey);
        int length = bb.getInt();
        byte[] tenantBytes = new byte[length];
        bb.get(tenantBytes);
        return new RawTenantAndPartition(tenantBytes, bb.getInt());
    }

    @Override
    public void changes(RowsChanged rowsChanged) throws Exception {
        if (rowsChanged.getVersionedPartitionName().equals(PartitionProvider.RING_INDEX)) {
            ringInitialized.set(false);
        }
    }

    @Override
    public void heartbeat(MiruHost miruHost) throws Exception {
        byte[] key = hostMarshaller.toBytes(miruHost);

        hostsClient().commit(new AmzaPartitionUpdates().set(key, FilerIO.longBytes(System.currentTimeMillis())), 0, 0, TimeUnit.MILLISECONDS);
    }

    @Override
    public LinkedHashSet<HostHeartbeat> getAllHosts() throws Exception {
        final LinkedHashSet<HostHeartbeat> heartbeats = new LinkedHashSet<>();
        hostsClient().scan(null, null, (transactionId, key, value) -> {
            MiruHost host = hostMarshaller.fromBytes(key);
            long timestamp = FilerIO.bytesLong(value.getValue());
            heartbeats.add(new HostHeartbeat(host, timestamp));
            return true;
        });
        return heartbeats;
    }

    @Override
    public void updateIngress(MiruIngressUpdate ingressUpdate) throws Exception {
        AmzaKretr ingressClient = ingressClient();
        AmzaPartitionUpdates updates = new AmzaPartitionUpdates();

        MiruTenantId tenantId = ingressUpdate.tenantId;
        MiruPartitionId partitionId = ingressUpdate.partitionId;

        IngressUpdate existing = ingressUpdate.absolute ? null : lookupIngress(tenantId, partitionId);

        if (ingressUpdate.ingressTimestamp != -1 &&
            (existing == null || existing.ingressTimestamp == -1 || ingressUpdate.ingressTimestamp > existing.ingressTimestamp)) {
            updates.set(toIngressKey(tenantId, partitionId, IngressType.ingressTimestamp),
                FilerIO.longBytes(ingressUpdate.ingressTimestamp));
        }
        if (ingressUpdate.minMax.clockMin != -1 &&
            (existing == null || existing.clockMin == -1 || ingressUpdate.minMax.clockMin < existing.clockMin)) {
            updates.set(toIngressKey(tenantId, partitionId, IngressType.clockMin), FilerIO.longBytes(ingressUpdate.minMax.clockMin));
        }
        if (ingressUpdate.minMax.clockMax != -1 &&
            (existing == null || existing.clockMax == -1 || ingressUpdate.minMax.clockMax > existing.clockMax)) {
            updates.set(toIngressKey(tenantId, partitionId, IngressType.clockMax), FilerIO.longBytes(ingressUpdate.minMax.clockMax));
        }
        if (ingressUpdate.minMax.orderIdMin != -1 &&
            (existing == null || existing.orderIdMin == -1 || ingressUpdate.minMax.orderIdMin < existing.orderIdMin)) {
            updates.set(toIngressKey(tenantId, partitionId, IngressType.orderIdMin), FilerIO.longBytes(ingressUpdate.minMax.orderIdMin));
        }
        if (ingressUpdate.minMax.orderIdMax != -1 &&
            (existing == null || existing.orderIdMax == -1 || ingressUpdate.minMax.orderIdMax > existing.orderIdMax)) {
            updates.set(toIngressKey(tenantId, partitionId, IngressType.orderIdMax), FilerIO.longBytes(ingressUpdate.minMax.orderIdMax));
        }

        ingressClient.commit(updates, replicateTakeQuorum, replicateTimeoutMillis, TimeUnit.MILLISECONDS);
    }

    @Override
    public void updateTopologies(MiruHost host, Collection<TopologyUpdate> topologyUpdates) throws Exception {
        final AmzaKretr topologyInfoClient = topologyInfoClient(host);
        AmzaPartitionUpdates updates = new AmzaPartitionUpdates();

        for (TopologyUpdate topologyUpdate : topologyUpdates) {
            MiruPartitionCoord coord = topologyUpdate.coord;
            Optional<MiruPartitionCoordInfo> optionalInfo = topologyUpdate.optionalInfo;
            Optional<Long> refreshQueryTimestamp = topologyUpdate.queryTimestamp;
            byte[] key = toTopologyKey(coord.tenantId, coord.partitionId);

            MiruPartitionCoordInfo coordInfo;
            if (optionalInfo.isPresent()) {
                coordInfo = optionalInfo.get();
            } else {
                byte[] got = topologyInfoClient.getValue(key);
                if (got != null) {
                    MiruTopologyColumnValue value = topologyColumnValueMarshaller.fromBytes(got);
                    coordInfo = new MiruPartitionCoordInfo(value.state, value.storage);
                } else {
                    coordInfo = new MiruPartitionCoordInfo(MiruPartitionState.offline, MiruBackingStorage.memory);
                }
            }
            long queryTimestamp = refreshQueryTimestamp.or(-1L);
            if (queryTimestamp == -1) {
                byte[] got = topologyInfoClient.getValue(key);
                if (got != null) {
                    MiruTopologyColumnValue value = topologyColumnValueMarshaller.fromBytes(got);
                    queryTimestamp = value.lastQueryTimestamp;
                } else {
                    queryTimestamp = 0;
                }
            }

            MiruTopologyColumnValue value = new MiruTopologyColumnValue(coordInfo.state, coordInfo.storage, queryTimestamp);
            LOG.debug("Updating {} to {} at query={}", new Object[] { coord, coordInfo, queryTimestamp });

            byte[] valueBytes = topologyColumnValueMarshaller.toBytes(value);
            updates.set(toTopologyKey(topologyUpdate.coord.tenantId, topologyUpdate.coord.partitionId), valueBytes);
        }

        topologyInfoClient.commit(updates, 0, 0, TimeUnit.MILLISECONDS);

        markTenantTopologyUpdated(topologyUpdates.stream()
            .filter(input -> input.optionalInfo.isPresent())
            .map(input -> input.coord.tenantId)
            .collect(Collectors.toSet()));
    }

    private void markTenantTopologyUpdated(Set<MiruTenantId> tenantIds) throws Exception {
        AmzaPartitionUpdates updates = new AmzaPartitionUpdates();
        for (MiruTenantId tenantId : tenantIds) {
            updates.set(toTenantKey(tenantId), EMPTY_BYTES);
        }

        for (HostHeartbeat heartbeat : getAllHosts()) {
            AmzaKretr topologyUpdatesClient = topologyUpdatesClient(heartbeat.host);
            topologyUpdatesClient.commit(updates, 0, 0, TimeUnit.MILLISECONDS);
        }
    }

    @Override
    public NamedCursorsResult<Collection<MiruTenantTopologyUpdate>> getTopologyUpdatesForHost(MiruHost host,
        Collection<NamedCursor> sinceCursors)
        throws Exception {

        final Map<MiruTenantId, MiruTenantTopologyUpdate> updates = Maps.newHashMap();

        String ringMember = amzaService.getRingReader().getRingMember().getMember();
        long sinceTransactionId = 0;
        for (NamedCursor sinceCursor : sinceCursors) {
            if (ringMember.equals(sinceCursor.name)) {
                sinceTransactionId = sinceCursor.id;
                break;
            }
        }

        AmzaKretr topologyUpdatesClient = topologyUpdatesClient(host);
        TakeCursors takeCursors = topologyUpdatesClient.takeFromTransactionId(sinceTransactionId, (transactionId, key, value) -> {
            MiruTenantId tenantId = fromTenantKey(key);
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
            for (TakeCursors.RingMemberCursor cursor : takeCursors.ringMemberCursors) {
                cursors.add(new NamedCursor(cursor.ringMember.getMember(), cursor.transactionId));
            }
        }

        return new NamedCursorsResult<>(cursors, updates.values());
    }

    @Override
    public NamedCursorsResult<Collection<MiruPartitionActiveUpdate>> getPartitionActiveUpdatesForHost(MiruHost host, Collection<NamedCursor> sinceCursors)
        throws Exception {

        AmzaKretr registryClient = registryClient(host);
        AmzaKretr topologyInfoClient = topologyInfoClient(host);
        AmzaKretr ingressClient = ingressClient();

        String partitionRegistryName = "host-" + host.toStringForm() + "-" + REGISTRY_SUFFIX;
        String partitionInfoName = "host-" + host.toStringForm() + "-" + INFO_SUFFIX;
        String partitionIngressName = "host-" + host.toStringForm() + "-" + INGRESS_PARTITION_NAME; // global region still gets host prefix

        String ringMember = amzaService.getRingReader().getRingMember().getMember();
        String registryCursorName = ringMember + '/' + partitionRegistryName;
        String infoCursorName = ringMember + '/' + partitionInfoName;
        String ingressCursorName = ringMember + '/' + partitionIngressName;

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
        TakeCursors registryTakeCursors = registryClient.takeFromTransactionId(registryTransactionId, (transactionId, key, value) -> {
            RawTenantAndPartition tenantAndPartition = fromTopologyKey(key);
            tenantPartitions.add(tenantAndPartition);
            return true;
        });
        TakeCursors infoTakeCursors = topologyInfoClient.takeFromTransactionId(infoTransactionId, (transactionId, key, value) -> {
            RawTenantAndPartition tenantAndPartition = fromTopologyKey(key);
            tenantPartitions.add(tenantAndPartition);
            return true;
        });
        TakeCursors ingressTakeCursors = ingressClient.takeFromTransactionId(ingressTransactionId, (transactionId, key, value) -> {
            RawTenantAndPartition tenantAndPartition = fromTopologyKey(key);
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
                MinMaxPriorityQueue<HostAndTimestamp> replicaSet = replicaSets.get(partitionId);
                if (replicaSet != null) {
                    for (HostAndTimestamp hostAndTimestamp : replicaSet) {
                        if (hostAndTimestamp.host.equals(host)) {
                            hosted = true;
                            break;
                        }
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
        for (TakeCursors.RingMemberCursor memberCursor : registryTakeCursors.ringMemberCursors) {
            String name = memberCursor.ringMember.getMember() + '/' + cursorName;
            NamedCursor existing = cursors.get(name);
            if (existing == null || existing.id < memberCursor.transactionId) {
                cursors.put(name, new NamedCursor(name, memberCursor.transactionId));
            }
        }
    }

    @Override
    public void ensurePartitionCoords(ListMultimap<MiruHost, TenantAndPartition> coords) throws Exception {
        for (MiruHost host : coords.keySet()) {
            AmzaKretr topologyInfoClient = topologyInfoClient(host);
            List<TenantAndPartition> tenantAndPartitions = coords.get(host);

            AmzaPartitionUpdates updates = new AmzaPartitionUpdates();
            // where's my putIfAbsent?
            topologyInfoClient.get(stream -> {
                for (TenantAndPartition tenantAndPartition : tenantAndPartitions) {
                    if (!stream.stream(toTopologyKey(tenantAndPartition.tenantId, tenantAndPartition.partitionId))) {
                        return false;
                    }
                }
                return true;
            }, (key, value, timestamp) -> {
                if (value == null) {
                    updates.set(key, initialEnsuredTopologyBytes);
                }
                return true;
            });

            topologyInfoClient.commit(updates, replicateTakeQuorum, replicateTimeoutMillis, TimeUnit.MILLISECONDS);
        }
    }

    @Override
    public void addToReplicaRegistry(ListMultimap<MiruHost, TenantAndPartition> coords, long nextId) throws Exception {
        Set<MiruTenantId> tenantIds = Sets.newHashSet();
        for (MiruHost host : coords.keySet()) {
            AmzaKretr registryClient = registryClient(host);
            List<TenantAndPartition> tenantAndPartitions = coords.get(host);
            AmzaPartitionUpdates updates = new AmzaPartitionUpdates();
            byte[] nextIdBytes = FilerIO.longBytes(nextId); // most recent is smallest.
            for (TenantAndPartition tenantAndPartition : tenantAndPartitions) {
                tenantIds.add(tenantAndPartition.tenantId);
                updates.set(toTopologyKey(tenantAndPartition.tenantId, tenantAndPartition.partitionId), nextIdBytes);
            }
            registryClient.commit(updates, replicateTakeQuorum, replicateTimeoutMillis, TimeUnit.MILLISECONDS);
        }
        markTenantTopologyUpdated(tenantIds);
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
        AmzaKretr registryClient = registryClient(host);
        registryClient.scan(null, null, (transactionId, key, value) -> {
            RawTenantAndPartition topologyKey = fromTopologyKey(key);
            tenants.add(new MiruTenantId(topologyKey.tenantBytes));
            return true;
        });
        return tenants;
    }

    @Override
    public void removeTenantPartionReplicaSet(final MiruTenantId tenantId, final MiruPartitionId partitionId) throws Exception {
        List<MiruHost> hosts = Lists.newArrayList();
        hostsClient().scan(null, null, (transactionId, key, value) -> {
            MiruHost host = hostMarshaller.fromBytes(key);
            hosts.add(host);
            return true;
        });
        for (MiruHost host : hosts) {
            AmzaKretr registryClient = registryClient(host);
            AmzaKretr topologyInfoClient = topologyInfoClient(host);
            byte[] topologyKey = toTopologyKey(tenantId, partitionId);
            registryClient.commit(new AmzaPartitionUpdates().remove(topologyKey), replicateTakeQuorum, replicateTimeoutMillis, TimeUnit.MILLISECONDS);
            topologyInfoClient.commit(new AmzaPartitionUpdates().remove(topologyKey), replicateTakeQuorum, replicateTimeoutMillis, TimeUnit.MILLISECONDS);
        }

        markTenantTopologyUpdated(Collections.singleton(tenantId));
    }

    @Override
    public List<MiruPartition> getPartitionsForTenant(MiruTenantId tenantId) throws Exception {
        NavigableMap<MiruPartitionId, MinMaxPriorityQueue<HostAndTimestamp>> partitionIdToLatest = tenantLatestTopologies(tenantId);

        List<MiruPartition> partitions = new ArrayList<>();
        for (MiruPartitionId partitionId : partitionIdToLatest.keySet()) {
            MinMaxPriorityQueue<HostAndTimestamp> got = partitionIdToLatest.get(partitionId);
            for (HostAndTimestamp hat : got) {
                AmzaKretr topologyInfoClient = topologyInfoClient(hat.host);
                byte[] rawInfo = topologyInfoClient.getValue(toTopologyKey(tenantId, partitionId));
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
            AmzaKretr registryClient = registryClient(hostHeartbeat.host);
            byte[] got = registryClient.getValue(toTopologyKey(tenantId, partitionId));
            if (got != null) {
                latest.add(new HostAndTimestamp(hostHeartbeat.host, FilerIO.bytesLong(got)));
            }
        }
        return latest;
    }

    private NavigableMap<MiruPartitionId, MinMaxPriorityQueue<HostAndTimestamp>> tenantLatestTopologies(MiruTenantId tenantId) throws Exception {
        final byte[] from = topologyKeyPrefix(tenantId);
        final byte[] to = WALKey.prefixUpperExclusive(from);
        final NavigableMap<MiruPartitionId, MinMaxPriorityQueue<HostAndTimestamp>> partitionIdToLatest = new TreeMap<>();
        for (HostHeartbeat hostHeartbeat : getAllHosts()) {
            AmzaKretr registryClient = registryClient(hostHeartbeat.host);
            registryClient.scan(from, to, (topologyTransactionId, topologyKey, topologyValue) -> {
                RawTenantAndPartition tenantPartitionKey = fromTopologyKey(topologyKey);
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
            AmzaKretr registryClient = registryClient(hostHeartbeat.host);
            for (MiruPartitionId partitionId : partitionIds) {
                byte[] got = registryClient.getValue(toTopologyKey(tenantId, partitionId));
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
                    AmzaKretr topologyInfoClient = topologyInfoClient(hat.host);
                    byte[] rawInfo = topologyInfoClient.getValue(toTopologyKey(tenantId, partitionId));
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
    public List<MiruPartitionStatus> getPartitionStatusForTenant(MiruTenantId tenantId, MiruPartitionId largestPartitionId) throws Exception {
        List<MiruPartitionStatus> status = new ArrayList<>();
        for (MiruPartitionId partitionId = MiruPartitionId.of(0); partitionId.compareTo(largestPartitionId) <= 0; partitionId = partitionId.next()) {
            long destroyAfterTimestamp = getDestroyAfterTimestamp(tenantId, partitionId);
            long lastIngressTimestampMillis = getIngressUpdate(tenantId, partitionId, IngressType.ingressTimestamp, 0);
            status.add(new MiruPartitionStatus(tenantId, partitionId, lastIngressTimestampMillis, destroyAfterTimestamp));
        }
        return status;
    }

    @Override
    public List<MiruTopologyStatus> getTopologyStatusForTenant(MiruTenantId tenantId) throws Exception {
        NavigableMap<MiruPartitionId, MinMaxPriorityQueue<HostAndTimestamp>> partitionIdToLatest = tenantLatestTopologies(tenantId);
        List<MiruTopologyStatus> status = new ArrayList<>();
        for (MiruPartitionId partitionId : partitionIdToLatest.keySet()) {
            MinMaxPriorityQueue<HostAndTimestamp> got = partitionIdToLatest.get(partitionId);
            for (HostAndTimestamp hat : got) {
                AmzaKretr topologyInfoClient = topologyInfoClient(hat.host);
                long lastIngressTimestampMillis = getIngressUpdate(tenantId, partitionId, IngressType.ingressTimestamp, 0);
                long lastQueryTimestampMillis = 0;
                byte[] rawInfo = topologyInfoClient.getValue(toTopologyKey(tenantId, partitionId));
                MiruPartitionCoordInfo info;
                if (rawInfo == null) {
                    info = new MiruPartitionCoordInfo(MiruPartitionState.offline, MiruBackingStorage.memory);
                } else {
                    MiruTopologyColumnValue columnValue = topologyColumnValueMarshaller.fromBytes(rawInfo);
                    info = new MiruPartitionCoordInfo(columnValue.state, columnValue.storage);
                    lastQueryTimestampMillis = columnValue.lastQueryTimestamp;
                }
                MiruPartitionCoord coord = new MiruPartitionCoord(tenantId, partitionId, hat.host);
                MiruPartition miruPartition = new MiruPartition(coord, info);
                status.add(new MiruTopologyStatus(miruPartition, lastIngressTimestampMillis, lastQueryTimestampMillis,
                    getDestroyAfterTimestamp(coord.tenantId, coord.partitionId)));
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
                    AmzaKretr topologyInfoClient = topologyInfoClient(hat.host);
                    byte[] rawInfo = topologyInfoClient.getValue(toTopologyKey(tenantId, partitionId));
                    MiruPartitionCoordInfo info;
                    long lastIngressTimestampMillis = getIngressUpdate(tenantId, partitionId, IngressType.ingressTimestamp, 0);
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
                    status.add(new MiruTopologyStatus(miruPartition, lastIngressTimestampMillis, lastQueryTimestampMillis,
                        getDestroyAfterTimestamp(coord.tenantId, coord.partitionId)));
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
            AmzaKretr topologyInfoClient = topologyInfoClient(hat.host);
            byte[] rawInfo = topologyInfoClient.getValue(toTopologyKey(tenantId, partitionId));
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
                    AmzaKretr topologyInfoClient = topologyInfoClient(hat.host);
                    byte[] rawInfo = topologyInfoClient.getValue(toTopologyKey(tenantId, partitionId));
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
        long lastIngressTimestamp = getIngressUpdate(coord.tenantId, coord.partitionId, IngressType.ingressTimestamp, -1L);
        long lastQueryTimestamp = -1;

        byte[] key = toTopologyKey(coord.tenantId, coord.partitionId);
        AmzaKretr topologyInfoClient = topologyInfoClient(coord.host);
        byte[] gotTopology = topologyInfoClient.getValue(key);
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

        return new MiruPartitionActive(activeUntilTimestamp, idleAfterTimestamp, getDestroyAfterTimestamp(coord.tenantId, coord.partitionId));
    }

    private long getDestroyAfterTimestamp(MiruTenantId tenantId, MiruPartitionId partitionId) throws Exception {
        long destroyAfterTimestamp = -1;
        long clockMax = getIngressUpdate(tenantId, partitionId, IngressType.clockMax, -1);
        if (clockMax > 0) {
            destroyAfterTimestamp = clockMax + defaultTopologyDestroyAfterMillis;
        }
        return destroyAfterTimestamp;
    }

    @Override
    public void removeHost(MiruHost host) throws Exception {
        Set<MiruTenantId> tenantIds = getTenantsForHostAsSet(host);

        // TODO add to Amza removeTable
        hostsClient().commit(new AmzaPartitionUpdates().remove(hostMarshaller.toBytes(host)),
            replicateTakeQuorum, replicateTimeoutMillis, TimeUnit.MILLISECONDS);

        markTenantTopologyUpdated(tenantIds);
    }

    @Override
    public void removeTopology(MiruTenantId tenantId, MiruPartitionId partitionId, MiruHost host) throws Exception {
        AmzaKretr topologyInfoClient = topologyInfoClient(host);
        topologyInfoClient.commit(new AmzaPartitionUpdates().remove(toTopologyKey(tenantId, partitionId)),
            replicateTakeQuorum, replicateTimeoutMillis, TimeUnit.MILLISECONDS);
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
        byte[] got = schemasClient().getValue(toTenantKey(tenantId));
        if (got == null) {
            return null;
        }
        return schemaMarshaller.fromBytes(got);
    }

    @Override
    public void registerSchema(MiruTenantId tenantId, MiruSchema schema) throws Exception {
        schemasClient().commit(new AmzaPartitionUpdates().set(toTenantKey(tenantId), schemaMarshaller.toBytes(schema)),
            replicateTakeQuorum, replicateTimeoutMillis, TimeUnit.MILLISECONDS);
    }

    @Override
    public boolean copySchema(MiruTenantId fromTenantId, List<MiruTenantId> toTenantIds) throws Exception {
        MiruSchema schema = getSchema(fromTenantId);
        if (schema == null) {
            return false;
        }
        byte[] schemaBytes = schemaMarshaller.toBytes(schema);
        AmzaPartitionUpdates updates = new AmzaPartitionUpdates();
        for (MiruTenantId to : toTenantIds) {
            updates.set(toTenantKey(to), schemaBytes);
        }
        schemasClient().commit(updates, replicateTakeQuorum, replicateTimeoutMillis, TimeUnit.MILLISECONDS);
        return true;
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

    private long getIngressUpdate(MiruTenantId tenantId, MiruPartitionId partitionId, IngressType type, long defaultValue) throws Exception {
        long value = defaultValue;
        AmzaKretr ingressClient = ingressClient();
        if (ingressClient != null) {
            byte[] clockMaxBytes = ingressClient.getValue(toIngressKey(tenantId, partitionId, type));
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
        byte[] fromKey = toIngressKey(tenantId, partitionId, null);
        byte[] toKey = WALKey.prefixUpperExclusive(fromKey);
        AmzaKretr ingressClient = ingressClient();
        if (ingressClient != null) {
            ingressClient.scan(fromKey, toKey, (rowTxId, key, value) -> {
                if (value != null) {
                    MiruPartitionId streamPartitionId = ingressKeyToPartitionId(key);
                    if (partitionId == null || partitionId.equals(streamPartitionId)) {
                        IngressType ingressType = ingressKeyToType(key);
                        if (streamRangeLookup.stream(streamPartitionId, ingressType, value.getTimestampId())) {
                            return true;
                        }
                    }
                }
                return false;
            });
        }
    }

    private byte[] toIngressKey(MiruTenantId tenantId, MiruPartitionId partitionId, IngressType ingressType) {
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
        return keyBytes;
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
