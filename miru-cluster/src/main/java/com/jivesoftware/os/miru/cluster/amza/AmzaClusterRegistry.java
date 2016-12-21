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
import com.jivesoftware.os.amza.api.TimestampedValue;
import com.jivesoftware.os.amza.api.filer.UIO;
import com.jivesoftware.os.amza.api.partition.Consistency;
import com.jivesoftware.os.amza.api.partition.Durability;
import com.jivesoftware.os.amza.api.partition.PartitionName;
import com.jivesoftware.os.amza.api.partition.PartitionProperties;
import com.jivesoftware.os.amza.api.scan.RowChanges;
import com.jivesoftware.os.amza.api.scan.RowsChanged;
import com.jivesoftware.os.amza.api.stream.RowType;
import com.jivesoftware.os.amza.api.stream.TxKeyValueStream.TxResult;
import com.jivesoftware.os.amza.api.take.TakeCursors;
import com.jivesoftware.os.amza.api.wal.WALKey;
import com.jivesoftware.os.amza.service.AmzaPartitionUpdates;
import com.jivesoftware.os.amza.service.AmzaService;
import com.jivesoftware.os.amza.service.EmbeddedClientProvider;
import com.jivesoftware.os.amza.service.EmbeddedClientProvider.CheckOnline;
import com.jivesoftware.os.amza.service.EmbeddedClientProvider.EmbeddedClient;
import com.jivesoftware.os.amza.service.Partition.ScanRange;
import com.jivesoftware.os.amza.service.storage.PartitionCreator;
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
import com.jivesoftware.os.miru.api.topology.MiruClusterClient.PartitionRange;
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
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
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

    private static final String LASTID_PARTITION_NAME = "partition-lastid-v2";
    private static final String INGRESS_PARTITION_NAME = "partition-ingress";
    private static final String DESTRUCTION_PARTITION_NAME = "partition-destruction";
    private static final String HOSTS_PARTITION_NAME = "hosts";
    private static final String SCHEMAS_PARTITION_NAME = "schemas";

    private static final String REGISTRY_SUFFIX = "partition-registry-v2";
    private static final String INFO_SUFFIX = "partition-info-v2";
    private static final String UPDATES_SUFFIX = "topology-updates-v2";

    private static final PartitionProperties CONSISTENT_PROPERTIES = new PartitionProperties(Durability.fsync_async, 0, 0, 0, 0, 0, 0, 0, 0, false,
        Consistency.quorum, true, true, false, RowType.primary, "lab", 16, null, -1, -1);

    private static final PartitionProperties EVENTUAL_PROPERTIES = new PartitionProperties(Durability.fsync_async, 0, 0, 0, 0, 0, 0, 0, 0, false,
        Consistency.none, false, true, false, RowType.primary, "lab", 16, null, -1, -1);

    private final MiruHostMarshaller hostMarshaller = new MiruHostMarshaller();
    private final MiruTopologyColumnValueMarshaller topologyColumnValueMarshaller = new MiruTopologyColumnValueMarshaller();
    private final TypeMarshaller<MiruSchema> schemaMarshaller;

    private final byte[] initialEnsuredTopologyBytes = topologyColumnValueMarshaller.toBytes(
        new MiruTopologyColumnValue(MiruPartitionState.offline, MiruBackingStorage.memory, 0));

    private final AmzaService amzaService;
    private final EmbeddedClientProvider embeddedClientProvider;
    private final long replicateTimeoutMillis;
    private final int defaultNumberOfReplicas;
    private final long defaultTopologyIsStaleAfterMillis;
    private final long defaultTopologyIsIdleAfterMillis;
    private final long defaultTopologyDestroyAfterMillis;
    private final long defaultTopologyCleanupAfterMillis;

    private final AtomicBoolean ringInitialized = new AtomicBoolean(false);
    private final ConcurrentMap<String, EmbeddedClient> clientMap = Maps.newConcurrentMap();

    public AmzaClusterRegistry(AmzaService amzaService,
        EmbeddedClientProvider embeddedClientProvider,
        long replicateTimeoutMillis,
        TypeMarshaller<MiruSchema> schemaMarshaller,
        int defaultNumberOfReplicas,
        long defaultTopologyIsStaleAfterMillis,
        long defaultTopologyIsIdleAfterMillis,
        long defaultTopologyDestroyAfterMillis,
        long defaultTopologyCleanupAfterMillis) throws Exception {
        this.amzaService = amzaService;
        this.embeddedClientProvider = embeddedClientProvider;
        this.replicateTimeoutMillis = replicateTimeoutMillis;
        this.schemaMarshaller = schemaMarshaller;
        this.defaultNumberOfReplicas = defaultNumberOfReplicas;
        this.defaultTopologyIsStaleAfterMillis = defaultTopologyIsStaleAfterMillis;
        this.defaultTopologyIsIdleAfterMillis = defaultTopologyIsIdleAfterMillis;
        this.defaultTopologyDestroyAfterMillis = defaultTopologyDestroyAfterMillis;
        this.defaultTopologyCleanupAfterMillis = defaultTopologyCleanupAfterMillis;
    }

    private EmbeddedClient ensureClient(String name, PartitionProperties partitionProperties) throws Exception {
        return clientMap.computeIfAbsent(name, s -> {
            try {
                amzaService.getRingWriter().ensureMaximalRing(CLUSTER_REGISTRY_RING_NAME, 10_000L); //TODO config
                PartitionName partitionName = new PartitionName(false, CLUSTER_REGISTRY_RING_NAME, name.getBytes(Charsets.UTF_8));
                amzaService.createPartitionIfAbsent(partitionName, partitionProperties);
                amzaService.awaitOnline(partitionName, 10_000L); //TODO config
                return embeddedClientProvider.getClient(partitionName, CheckOnline.once);
            } catch (Exception e) {
                throw new RuntimeException("Failed to get amza client", e);
            }
        });
    }

    private EmbeddedClient hostsClient() throws Exception {
        return ensureClient(HOSTS_PARTITION_NAME, EVENTUAL_PROPERTIES);
    }

    private EmbeddedClient lastIdClient() throws Exception {
        return ensureClient(LASTID_PARTITION_NAME, CONSISTENT_PROPERTIES);
    }

    private EmbeddedClient ingressClient() throws Exception {
        return ensureClient(INGRESS_PARTITION_NAME, CONSISTENT_PROPERTIES);
    }

    private EmbeddedClient destructionClient() throws Exception {
        return ensureClient(DESTRUCTION_PARTITION_NAME, CONSISTENT_PROPERTIES);
    }

    private EmbeddedClient schemasClient() throws Exception {
        return ensureClient(SCHEMAS_PARTITION_NAME, CONSISTENT_PROPERTIES);
    }

    private EmbeddedClient topologyInfoClient(MiruHost host) throws Exception {
        return ensureClient("host-" + host.getLogicalName() + "-" + INFO_SUFFIX, EVENTUAL_PROPERTIES);
    }

    private EmbeddedClient topologyUpdatesClient(MiruHost host) throws Exception {
        return ensureClient("host-" + host.getLogicalName() + "-" + UPDATES_SUFFIX, EVENTUAL_PROPERTIES);
    }

    private EmbeddedClient registryClient(MiruHost host) throws Exception {
        return ensureClient("host-" + host.getLogicalName() + "-" + REGISTRY_SUFFIX, CONSISTENT_PROPERTIES);
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
        if (rowsChanged.getVersionedPartitionName().equals(PartitionCreator.RING_INDEX)) {
            ringInitialized.set(false);
        }
    }

    @Override
    public void heartbeat(MiruHost miruHost) throws Exception {
        byte[] key = hostMarshaller.toBytes(miruHost);

        hostsClient().commit(Consistency.none, null, new AmzaPartitionUpdates().set(key, FilerIO.longBytes(System.currentTimeMillis()), -1), 10,
            TimeUnit.SECONDS);
    }

    @Override
    public LinkedHashSet<HostHeartbeat> getAllHosts() throws Exception {
        final LinkedHashSet<HostHeartbeat> heartbeats = new LinkedHashSet<>();
        hostsClient().scan(
            Collections.singletonList(new ScanRange(null, null, null, null)),
            (prefix, key, value, timestamp, version) -> {
                MiruHost host = hostMarshaller.fromBytes(key);
                long valueAsTimestamp = FilerIO.bytesLong(value);
                heartbeats.add(new HostHeartbeat(host, valueAsTimestamp));
                return true;
            },
            true
        );
        return heartbeats;
    }

    @Override
    public void updateIngress(MiruIngressUpdate ingressUpdate) throws Exception {
        EmbeddedClient ingressClient = ingressClient();
        AmzaPartitionUpdates updates = new AmzaPartitionUpdates();

        MiruTenantId tenantId = ingressUpdate.tenantId;
        MiruPartitionId partitionId = ingressUpdate.partitionId;

        IngressUpdate existing = ingressUpdate.absolute ? null : lookupIngress(tenantId, partitionId);

        if (ingressUpdate.ingressTimestamp != -1
            && (existing == null || existing.ingressTimestamp == -1 || ingressUpdate.ingressTimestamp > existing.ingressTimestamp)) {
            updates.set(toIngressKey(tenantId, partitionId, IngressType.ingressTimestamp),
                FilerIO.longBytes(ingressUpdate.ingressTimestamp), -1);
        }
        if (ingressUpdate.minMax.clockMin != -1
            && (existing == null || existing.clockMin == -1 || ingressUpdate.minMax.clockMin < existing.clockMin)) {
            updates.set(toIngressKey(tenantId, partitionId, IngressType.clockMin), FilerIO.longBytes(ingressUpdate.minMax.clockMin), -1);
        }
        if (ingressUpdate.minMax.clockMax != -1
            && (existing == null || existing.clockMax == -1 || ingressUpdate.minMax.clockMax > existing.clockMax)) {
            updates.set(toIngressKey(tenantId, partitionId, IngressType.clockMax), FilerIO.longBytes(ingressUpdate.minMax.clockMax), -1);
        }
        if (ingressUpdate.minMax.orderIdMin != -1
            && (existing == null || existing.orderIdMin == -1 || ingressUpdate.minMax.orderIdMin < existing.orderIdMin)) {
            updates.set(toIngressKey(tenantId, partitionId, IngressType.orderIdMin), FilerIO.longBytes(ingressUpdate.minMax.orderIdMin), -1);
        }
        if (ingressUpdate.minMax.orderIdMax != -1
            && (existing == null || existing.orderIdMax == -1 || ingressUpdate.minMax.orderIdMax > existing.orderIdMax)) {
            updates.set(toIngressKey(tenantId, partitionId, IngressType.orderIdMax), FilerIO.longBytes(ingressUpdate.minMax.orderIdMax), -1);
        }

        ingressClient.commit(Consistency.quorum, null, updates, replicateTimeoutMillis, TimeUnit.MILLISECONDS);
    }

    @Override
    public void removeIngress(MiruTenantId tenantId, MiruPartitionId partitionId) throws Exception {
        EmbeddedClient ingressClient = ingressClient();
        AmzaPartitionUpdates updates = new AmzaPartitionUpdates();
        updates.remove(toIngressKey(tenantId, partitionId, IngressType.ingressTimestamp), -1);
        updates.remove(toIngressKey(tenantId, partitionId, IngressType.clockMin), -1);
        updates.remove(toIngressKey(tenantId, partitionId, IngressType.clockMax), -1);
        updates.remove(toIngressKey(tenantId, partitionId, IngressType.orderIdMin), -1);
        updates.remove(toIngressKey(tenantId, partitionId, IngressType.orderIdMax), -1);
        ingressClient.commit(Consistency.quorum, null, updates, replicateTimeoutMillis, TimeUnit.MILLISECONDS);
    }

    @Override
    public void updateLastId(MiruPartitionCoord coord, int lastId) throws Exception {
        EmbeddedClient lastIdClient = lastIdClient();
        AmzaPartitionUpdates updates = new AmzaPartitionUpdates();
        updates.set(toLastIdKey(coord.tenantId, coord.partitionId, coord.host), UIO.intBytes(lastId), -1);
        lastIdClient.commit(Consistency.quorum, null, updates, replicateTimeoutMillis, TimeUnit.MILLISECONDS);
    }

    @Override
    public void destroyPartition(MiruTenantId tenantId, MiruPartitionId partitionId) throws Exception {
        EmbeddedClient destructionClient = destructionClient();
        AmzaPartitionUpdates updates = new AmzaPartitionUpdates();
        updates.set(toTopologyKey(tenantId, partitionId), FilerIO.longBytes(System.currentTimeMillis()), -1);
        destructionClient.commit(Consistency.quorum, null, updates, replicateTimeoutMillis, TimeUnit.MILLISECONDS);
    }

    @Override
    public void updateTopologies(MiruHost host, Collection<TopologyUpdate> topologyUpdates) throws Exception {
        final EmbeddedClient topologyInfoClient = topologyInfoClient(host);
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
                byte[] got = topologyInfoClient.getValue(Consistency.none, null, key);
                if (got != null) {
                    MiruTopologyColumnValue value = topologyColumnValueMarshaller.fromBytes(got);
                    coordInfo = new MiruPartitionCoordInfo(value.state, value.storage);
                } else {
                    coordInfo = new MiruPartitionCoordInfo(MiruPartitionState.offline, MiruBackingStorage.memory);
                }
            }
            long queryTimestamp = refreshQueryTimestamp.or(-1L);
            if (queryTimestamp == -1) {
                byte[] got = topologyInfoClient.getValue(Consistency.none, null, key);
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
            updates.set(toTopologyKey(topologyUpdate.coord.tenantId, topologyUpdate.coord.partitionId), valueBytes, -1);
        }

        topologyInfoClient.commit(Consistency.none, null, updates, replicateTimeoutMillis, TimeUnit.MILLISECONDS);

        markTenantTopologyUpdated(topologyUpdates.stream()
            .filter(input -> input.optionalInfo.isPresent())
            .map(input -> input.coord.tenantId)
            .collect(Collectors.toSet()));
    }

    private void markTenantTopologyUpdated(Set<MiruTenantId> tenantIds) throws Exception {
        AmzaPartitionUpdates updates = new AmzaPartitionUpdates();
        for (MiruTenantId tenantId : tenantIds) {
            updates.set(toTenantKey(tenantId), EMPTY_BYTES, -1);
        }

        for (HostHeartbeat heartbeat : getAllHosts()) {
            EmbeddedClient topologyUpdatesClient = topologyUpdatesClient(heartbeat.host);
            topologyUpdatesClient.commit(Consistency.none, null, updates, replicateTimeoutMillis, TimeUnit.MILLISECONDS);
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

        EmbeddedClient topologyUpdatesClient = topologyUpdatesClient(host);
        TakeCursors takeCursors = topologyUpdatesClient.takeFromTransactionId(sinceTransactionId,
            (long rowTxId, byte[] prefix, byte[] key, byte[] value, long valueTimestamp, boolean valueTombstoned, long valueVersion) -> {
                MiruTenantId tenantId = fromTenantKey(key);
                MiruTenantTopologyUpdate existing = updates.get(tenantId);
                if (existing == null || existing.timestamp < valueTimestamp) {
                    updates.put(tenantId, new MiruTenantTopologyUpdate(tenantId, valueTimestamp));
                }
                return TxResult.MORE;
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

        EmbeddedClient registryClient = registryClient(host);
        EmbeddedClient topologyInfoClient = topologyInfoClient(host);
        EmbeddedClient ingressClient = ingressClient();

        String partitionRegistryName = "host-" + host.getLogicalName() + "-" + REGISTRY_SUFFIX;
        String partitionInfoName = "host-" + host.getLogicalName() + "-" + INFO_SUFFIX;
        String partitionIngressName = "host-" + host.getLogicalName() + "-" + INGRESS_PARTITION_NAME; // global region still gets host prefix

        String ringMember = amzaService.getRingReader().getRingMember().getMember();
        String registryCursorName = ringMember + '/' + partitionRegistryName;
        String infoCursorName = ringMember + '/' + partitionInfoName;
        String ingressCursorName = ringMember + '/' + partitionIngressName;

        long[] registryTransactionId = new long[1];
        long[] infoTransactionId = new long[1];
        long[] ingressTransactionId = new long[1];
        Map<String, NamedCursor> cursors = Maps.newHashMapWithExpectedSize(sinceCursors.size());
        for (NamedCursor sinceCursor : sinceCursors) {
            cursors.put(sinceCursor.name, sinceCursor);
            if (registryCursorName.equals(sinceCursor.name)) {
                registryTransactionId[0] = sinceCursor.id;
            } else if (infoCursorName.equals(sinceCursor.name)) {
                infoTransactionId[0] = sinceCursor.id;
            } else if (ingressCursorName.equals(sinceCursor.name)) {
                ingressTransactionId[0] = sinceCursor.id;
            }
        }

        final Set<RawTenantAndPartition> tenantPartitions = Sets.newHashSet();
        int[] registryCount = new int[1];
        TakeCursors registryTakeCursors = registryClient.takeFromTransactionId(registryTransactionId[0],
            (long rowTxId, byte[] prefix, byte[] key, byte[] value, long valueTimestamp, boolean valueTombstoned, long valueVersion) -> {
                RawTenantAndPartition tenantAndPartition = fromTopologyKey(key);
                tenantPartitions.add(tenantAndPartition);
                registryCount[0]++;
                if (registryCount[0] % 10_000 == 0) {
                    LOG.info("Found {} registry updates for {} from txId {}", registryCount[0], host, registryTransactionId[0]);
                }
                return TxResult.MORE;
            });
        int[] infoCount = new int[1];
        TakeCursors infoTakeCursors = topologyInfoClient.takeFromTransactionId(infoTransactionId[0],
            (long rowTxId, byte[] prefix, byte[] key, byte[] value, long valueTimestamp, boolean valueTombstoned, long valueVersion) -> {
                RawTenantAndPartition tenantAndPartition = fromTopologyKey(key);
                tenantPartitions.add(tenantAndPartition);
                infoCount[0]++;
                if (infoCount[0] % 10_000 == 0) {
                    LOG.info("Found {} info updates for {} from txId {}", infoCount[0], host, infoTransactionId[0]);
                }
                return TxResult.MORE;
            });
        int[] ingressCount = new int[1];
        TakeCursors ingressTakeCursors = ingressClient.takeFromTransactionId(ingressTransactionId[0],
            (long rowTxId, byte[] prefix, byte[] key, byte[] value, long valueTimestamp, boolean valueTombstoned, long valueVersion) -> {
                RawTenantAndPartition tenantAndPartition = fromTopologyKey(key);
                tenantPartitions.add(tenantAndPartition);
                ingressCount[0]++;
                if (ingressCount[0] % 10_000 == 0) {
                    LOG.info("Found {} ingress updates for {} from txId {}", ingressCount[0], host, ingressTransactionId[0]);
                }
                return TxResult.MORE;
            });
        LOG.info("Returning {} active updates for {} using registry={} info={} ingress={}",
            tenantPartitions.size(), host, registryCount[0], infoCount[0], ingressCount[0]);

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
                updates.add(new MiruPartitionActiveUpdate(tenantId,
                    partitionId.getId(),
                    hosted,
                    partitionActive.activeUntilTimestamp,
                    partitionActive.idleAfterTimestamp,
                    partitionActive.destroyAfterTimestamp,
                    partitionActive.cleanupAfterTimestamp));
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
            EmbeddedClient topologyInfoClient = topologyInfoClient(host);
            List<TenantAndPartition> tenantAndPartitions = coords.get(host);

            AmzaPartitionUpdates updates = new AmzaPartitionUpdates();
            // where's my putIfAbsent?
            topologyInfoClient.get(Consistency.none, null, stream -> {
                for (TenantAndPartition tenantAndPartition : tenantAndPartitions) {
                    if (!stream.stream(toTopologyKey(tenantAndPartition.tenantId, tenantAndPartition.partitionId))) {
                        return false;
                    }
                }
                return true;
            }, (byte[] prefix, byte[] key, byte[] value, long timestamp, long version) -> {
                if (value == null) {
                    updates.set(key, initialEnsuredTopologyBytes, -1);
                }
                return true;
            });

            topologyInfoClient.commit(Consistency.none, null, updates, replicateTimeoutMillis, TimeUnit.MILLISECONDS);
        }
    }

    @Override
    public void addToReplicaRegistry(ListMultimap<MiruHost, TenantAndPartition> coords, long nextId) throws Exception {
        Set<MiruTenantId> tenantIds = Sets.newHashSet();
        for (MiruHost host : coords.keySet()) {
            EmbeddedClient registryClient = registryClient(host);
            List<TenantAndPartition> tenantAndPartitions = coords.get(host);
            AmzaPartitionUpdates updates = new AmzaPartitionUpdates();
            byte[] nextIdBytes = FilerIO.longBytes(nextId); // most recent is smallest.
            for (TenantAndPartition tenantAndPartition : tenantAndPartitions) {
                tenantIds.add(tenantAndPartition.tenantId);
                updates.set(toTopologyKey(tenantAndPartition.tenantId, tenantAndPartition.partitionId), nextIdBytes, -1);
            }
            registryClient.commit(Consistency.quorum, null, updates, replicateTimeoutMillis, TimeUnit.MILLISECONDS);
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
        EmbeddedClient registryClient = registryClient(host);
        registryClient.scan(
            Collections.singletonList(new ScanRange(null, null, null, null)),
            (prefix, key, value, timestamp, version) -> {
                RawTenantAndPartition topologyKey = fromTopologyKey(key);
                tenants.add(new MiruTenantId(topologyKey.tenantBytes));
                return true;
            },
            true
        );
        return tenants;
    }

    @Override
    public void removeTenantPartionReplicaSet(final MiruTenantId tenantId, final MiruPartitionId partitionId) throws Exception {
        List<MiruHost> hosts = Lists.newArrayList();
        hostsClient().scan(
            Collections.singletonList(new ScanRange(null, null, null, null)),
            (byte[] prefix, byte[] key, byte[] value, long timestamp, long version) -> {
                MiruHost host = hostMarshaller.fromBytes(key);
                hosts.add(host);
                return true;
            },
            true
        );
        for (MiruHost host : hosts) {
            EmbeddedClient registryClient = registryClient(host);
            EmbeddedClient topologyInfoClient = topologyInfoClient(host);
            byte[] topologyKey = toTopologyKey(tenantId, partitionId);
            registryClient.commit(Consistency.quorum, null, new AmzaPartitionUpdates().remove(topologyKey, -1),
                replicateTimeoutMillis, TimeUnit.MILLISECONDS);
            topologyInfoClient.commit(Consistency.none, null, new AmzaPartitionUpdates().remove(topologyKey, -1),
                replicateTimeoutMillis, TimeUnit.MILLISECONDS);
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
                EmbeddedClient topologyInfoClient = topologyInfoClient(hat.host);
                byte[] rawInfo = topologyInfoClient.getValue(Consistency.none, null, toTopologyKey(tenantId, partitionId));
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
            EmbeddedClient registryClient = registryClient(hostHeartbeat.host);
            byte[] got = registryClient.getValue(Consistency.quorum, null, toTopologyKey(tenantId, partitionId));
            if (got != null) {
                latest.add(new HostAndTimestamp(hostHeartbeat.host, FilerIO.bytesLong(got)));
            }
        }
        return latest;
    }

    @Override
    public void debugTenant(MiruTenantId tenantId, StringBuilder builder) throws Exception {
        final byte[] from = topologyKeyPrefix(tenantId);
        final byte[] to = WALKey.prefixUpperExclusive(from);
        for (HostHeartbeat hostHeartbeat : getAllHosts()) {
            EmbeddedClient registryClient = registryClient(hostHeartbeat.host);
            registryClient.scan(
                Collections.singletonList(new ScanRange(null, from, null, to)),
                (prefix, key, value, timestamp, version) -> {
                    RawTenantAndPartition tenantPartitionKey = fromTopologyKey(key);
                    MiruPartitionId partitionId = MiruPartitionId.of(tenantPartitionKey.partitionId);
                    builder.append("partition: ").append(partitionId.getId())
                        .append(", host: ").append(hostHeartbeat.host)
                        .append(", value: ").append(FilerIO.bytesLong(value))
                        .append(", timestamp: ").append(timestamp)
                        .append(", version: ").append(version)
                        .append("\n");
                    return true;
                },
                true
            );
        }
    }

    @Override
    public void debugTenantPartition(MiruTenantId tenantId, MiruPartitionId partitionId, StringBuilder builder) throws Exception {
        for (HostHeartbeat hostHeartbeat : getAllHosts()) {
            EmbeddedClient registryClient = registryClient(hostHeartbeat.host);
            TimestampedValue got = registryClient.getTimestampedValue(Consistency.quorum, null, toTopologyKey(tenantId, partitionId));
            if (got != null) {
                builder.append("host: ").append(hostHeartbeat.host)
                    .append(", value: ").append(FilerIO.bytesLong(got.getValue()))
                    .append(", timestamp: ").append(got.getTimestampId())
                    .append(", version: ").append(got.getVersion())
                    .append("\n");
            }
        }
    }

    private NavigableMap<MiruPartitionId, MinMaxPriorityQueue<HostAndTimestamp>> tenantLatestTopologies(MiruTenantId tenantId) throws Exception {
        final byte[] from = topologyKeyPrefix(tenantId);
        final byte[] to = WALKey.prefixUpperExclusive(from);
        final NavigableMap<MiruPartitionId, MinMaxPriorityQueue<HostAndTimestamp>> partitionIdToLatest = new TreeMap<>();
        for (HostHeartbeat hostHeartbeat : getAllHosts()) {
            EmbeddedClient registryClient = registryClient(hostHeartbeat.host);
            registryClient.scan(
                Collections.singletonList(new ScanRange(null, from, null, to)),
                (prefix, key, value, timestamp, version) -> {
                    RawTenantAndPartition tenantPartitionKey = fromTopologyKey(key);
                    MiruPartitionId partitionId = MiruPartitionId.of(tenantPartitionKey.partitionId);
                    MinMaxPriorityQueue<HostAndTimestamp> latest = partitionIdToLatest.get(partitionId);
                    if (latest == null) {
                        // TODO defaultNumberOfReplicas should come from config?
                        latest = MinMaxPriorityQueue.maximumSize(defaultNumberOfReplicas)
                            .expectedSize(defaultNumberOfReplicas)
                            .create();
                        partitionIdToLatest.put(partitionId, latest);
                    }
                    latest.add(new HostAndTimestamp(hostHeartbeat.host, FilerIO.bytesLong(value)));
                    return true;
                },
                true
            );
        }
        return partitionIdToLatest;
    }

    private NavigableMap<MiruPartitionId, MinMaxPriorityQueue<HostAndTimestamp>> tenantPartitionsLatestTopologies(MiruTenantId tenantId,
        Collection<MiruPartitionId> partitionIds) throws Exception {

        final NavigableMap<MiruPartitionId, MinMaxPriorityQueue<HostAndTimestamp>> partitionIdToLatest = new TreeMap<>();

        for (HostHeartbeat hostHeartbeat : getAllHosts()) {
            EmbeddedClient registryClient = registryClient(hostHeartbeat.host);
            for (MiruPartitionId partitionId : partitionIds) {
                byte[] got = registryClient.getValue(Consistency.quorum, null, toTopologyKey(tenantId, partitionId));
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
                    EmbeddedClient topologyInfoClient = topologyInfoClient(hat.host);
                    byte[] rawInfo = topologyInfoClient.getValue(Consistency.none, null, toTopologyKey(tenantId, partitionId));
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
            IngressStatusTimestamps ingressStatusTimestamps = getIngressStatusTimestamps(tenantId, partitionId);
            long lastIngressTimestampMillis = getIngressUpdate(tenantId, partitionId, IngressType.ingressTimestamp, 0);
            status.add(new MiruPartitionStatus(tenantId,
                partitionId,
                lastIngressTimestampMillis,
                ingressStatusTimestamps.destroyAfterTimestamp,
                ingressStatusTimestamps.cleanupAfterTimestamp));
        }
        return status;
    }

    @Override
    public List<MiruTopologyStatus> getTopologyStatusForTenant(MiruTenantId tenantId) throws Exception {
        Map<MiruPartitionCoord, long[]> lastIds = getLastIds(tenantId);

        NavigableMap<MiruPartitionId, MinMaxPriorityQueue<HostAndTimestamp>> partitionIdToLatest = tenantLatestTopologies(tenantId);
        List<MiruTopologyStatus> status = new ArrayList<>();
        for (MiruPartitionId partitionId : partitionIdToLatest.keySet()) {
            MinMaxPriorityQueue<HostAndTimestamp> got = partitionIdToLatest.get(partitionId);
            for (HostAndTimestamp hat : got) {
                EmbeddedClient topologyInfoClient = topologyInfoClient(hat.host);
                long lastIngressTimestampMillis = getIngressUpdate(tenantId, partitionId, IngressType.ingressTimestamp, 0);
                long lastQueryTimestampMillis = 0;
                byte[] rawInfo = topologyInfoClient.getValue(Consistency.none, null, toTopologyKey(tenantId, partitionId));
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

                long[] lastIdAndTimestamp = lastIds.getOrDefault(coord, NO_LASTID);
                IngressStatusTimestamps ingressStatusTimestamps = getIngressStatusTimestamps(coord.tenantId, coord.partitionId);
                status.add(new MiruTopologyStatus(miruPartition, lastIngressTimestampMillis, lastQueryTimestampMillis,
                    ingressStatusTimestamps.destroyAfterTimestamp,
                    ingressStatusTimestamps.cleanupAfterTimestamp,
                    (int) lastIdAndTimestamp[0],
                    lastIdAndTimestamp[1]));
            }
        }
        return status;
    }

    @Override
    public List<MiruTopologyStatus> getTopologyStatusForTenantHost(MiruTenantId tenantId, MiruHost host) throws Exception {
        Map<MiruPartitionCoord, long[]> lastIds = getLastIds(tenantId);

        NavigableMap<MiruPartitionId, MinMaxPriorityQueue<HostAndTimestamp>> partitionIdToLatest = tenantLatestTopologies(tenantId);
        List<MiruTopologyStatus> status = new ArrayList<>();
        for (MiruPartitionId partitionId : partitionIdToLatest.keySet()) {
            MinMaxPriorityQueue<HostAndTimestamp> got = partitionIdToLatest.get(partitionId);
            for (HostAndTimestamp hat : got) {
                if (hat.host.equals(host)) {
                    EmbeddedClient topologyInfoClient = topologyInfoClient(hat.host);
                    byte[] rawInfo = topologyInfoClient.getValue(Consistency.none, null, toTopologyKey(tenantId, partitionId));
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

                    long[] lastIdAndTimestamp = lastIds.getOrDefault(coord, NO_LASTID);
                    IngressStatusTimestamps ingressStatusTimestamps = getIngressStatusTimestamps(coord.tenantId, coord.partitionId);
                    status.add(new MiruTopologyStatus(miruPartition, lastIngressTimestampMillis, lastQueryTimestampMillis,
                        ingressStatusTimestamps.destroyAfterTimestamp,
                        ingressStatusTimestamps.cleanupAfterTimestamp,
                        (int) lastIdAndTimestamp[0],
                        lastIdAndTimestamp[1]));
                }
            }
        }
        return status;
    }

    private static final long[] NO_LASTID = { -1, -1 };

    private Map<MiruPartitionCoord, long[]> getLastIds(MiruTenantId tenantId) throws Exception {
        EmbeddedClient lastIdClient = lastIdClient();
        byte[] tenantKey = toLastIdKey(tenantId, null, null);
        Map<MiruPartitionCoord, long[]> lastIds = Maps.newHashMap();
        lastIdClient.scan(Collections.singletonList(new ScanRange(null, tenantKey, null, WALKey.prefixUpperExclusive(tenantKey))),
            (prefix, key, value, timestamp, version) -> {
                if (value != null) {
                    MiruPartitionCoord coord = lastIdKeyToCoord(key);
                    lastIds.put(coord, new long[] { (long) UIO.bytesInt(value), timestamp });
                }
                return true;
            }, true);
        return lastIds;
    }

    @Override
    public MiruReplicaSet getReplicaSet(MiruTenantId tenantId, MiruPartitionId partitionId) throws Exception {
        MinMaxPriorityQueue<HostAndTimestamp> latest = tenantLatestTopology(tenantId, partitionId);
        List<MiruPartition> partitions = Lists.newArrayList();
        Set<MiruHost> replicaHosts = Sets.newHashSet();

        for (HostAndTimestamp hat : latest) {
            EmbeddedClient topologyInfoClient = topologyInfoClient(hat.host);
            byte[] rawInfo = topologyInfoClient.getValue(Consistency.none, null, toTopologyKey(tenantId, partitionId));
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
        return new MiruReplicaSet(extractPartitionsByState(partitions), replicaHosts, missing, defaultNumberOfReplicas);
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
                    EmbeddedClient topologyInfoClient = topologyInfoClient(hat.host);
                    byte[] rawInfo = topologyInfoClient.getValue(Consistency.none, null, toTopologyKey(tenantId, partitionId));
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

        Map<MiruPartitionId, MiruReplicaSet> replicaSets = Maps.newHashMapWithExpectedSize(requiredPartitionId.size());
        for (MiruPartitionId partitionId : requiredPartitionId) {
            List<MiruPartition> partitions = partitionsPartitions.get(partitionId);
            Set<MiruHost> replicaHosts = partitionHosts.get(partitionId);
            int missing = defaultNumberOfReplicas - replicaHosts.size(); // TODO expose to config?
            replicaSets.put(partitionId, new MiruReplicaSet(extractPartitionsByState(partitions), replicaHosts, missing, defaultNumberOfReplicas));
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
        EmbeddedClient topologyInfoClient = topologyInfoClient(coord.host);
        byte[] gotTopology = topologyInfoClient.getValue(Consistency.none, null, key);
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

        IngressStatusTimestamps ingressStatusTimestamps = getIngressStatusTimestamps(coord.tenantId, coord.partitionId);
        return new MiruPartitionActive(activeUntilTimestamp,
            idleAfterTimestamp,
            ingressStatusTimestamps.destroyAfterTimestamp,
            ingressStatusTimestamps.cleanupAfterTimestamp);
    }

    private IngressStatusTimestamps getIngressStatusTimestamps(MiruTenantId tenantId, MiruPartitionId partitionId) throws Exception {
        long destroyAfterTimestamp = -1;
        long cleanupAfterTimestamp = -1;

        EmbeddedClient destructionClient = destructionClient();
        byte[] destructionValue = destructionClient.getValue(Consistency.quorum, null, toTopologyKey(tenantId, partitionId));
        if (destructionValue != null) {
            destroyAfterTimestamp = FilerIO.bytesLong(destructionValue);
        }

        long clockMax = getIngressUpdate(tenantId, partitionId, IngressType.clockMax, -1);
        if (clockMax != -1) {
            if (destroyAfterTimestamp == -1) {
                destroyAfterTimestamp = clockMax + defaultTopologyDestroyAfterMillis;
            }
            cleanupAfterTimestamp = clockMax + defaultTopologyCleanupAfterMillis;
        }
        return new IngressStatusTimestamps(destroyAfterTimestamp, cleanupAfterTimestamp);
    }

    private static class IngressStatusTimestamps {
        private final long destroyAfterTimestamp;
        private final long cleanupAfterTimestamp;

        public IngressStatusTimestamps(long destroyAfterTimestamp, long cleanupAfterTimestamp) {
            this.destroyAfterTimestamp = destroyAfterTimestamp;
            this.cleanupAfterTimestamp = cleanupAfterTimestamp;
        }
    }

    @Override
    public void removeHost(MiruHost host) throws Exception {
        Set<MiruTenantId> tenantIds = getTenantsForHostAsSet(host);

        // TODO add to Amza removeTable
        hostsClient().commit(Consistency.none, null, new AmzaPartitionUpdates().remove(hostMarshaller.toBytes(host), -1),
            replicateTimeoutMillis, TimeUnit.MILLISECONDS);

        markTenantTopologyUpdated(tenantIds);
    }

    @Override
    public void removeTopology(MiruTenantId tenantId, MiruPartitionId partitionId, MiruHost host) throws Exception {
        EmbeddedClient topologyInfoClient = topologyInfoClient(host);
        topologyInfoClient.commit(Consistency.none, null, new AmzaPartitionUpdates().remove(toTopologyKey(tenantId, partitionId), -1),
            replicateTimeoutMillis, TimeUnit.MILLISECONDS);
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
        byte[] got = schemasClient().getValue(Consistency.quorum, null, toTenantKey(tenantId));
        if (got == null) {
            return null;
        }
        return schemaMarshaller.fromBytes(got);
    }

    @Override
    public void registerSchema(MiruTenantId tenantId, MiruSchema schema) throws Exception {
        schemasClient().commit(Consistency.quorum, null, new AmzaPartitionUpdates().set(toTenantKey(tenantId), schemaMarshaller.toBytes(schema), -1),
            replicateTimeoutMillis, TimeUnit.MILLISECONDS);
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
            updates.set(toTenantKey(to), schemaBytes, -1);
        }
        schemasClient().commit(Consistency.quorum, null, updates, replicateTimeoutMillis, TimeUnit.MILLISECONDS);
        return true;
    }

    @Override
    public int upgradeSchema(MiruSchema schema, boolean upgradeOnMissing, boolean upgradeOnError) throws Exception {
        Set<MiruTenantId> toTenantIds = Sets.newHashSet();
        schemasClient().scan(
            Collections.singletonList(new ScanRange(null, null, null, null)),
            (prefix, key, value, timestamp, version) -> {
                toTenantIds.add(fromTenantKey(key));
                return true;
            },
            true
        );

        byte[] schemaBytes = schemaMarshaller.toBytes(schema);
        AmzaPartitionUpdates updates = new AmzaPartitionUpdates();
        for (MiruTenantId to : toTenantIds) {
            MiruSchema existing = null;
            boolean error = false;
            try {
                existing = getSchema(to);
            } catch (Exception e) {
                LOG.warn("Failed to deserialize schema for {}", to);
                error = true;
            }
            if (existing != null && existing.getName().equals(schema.getName()) && existing.getVersion() != schema.getVersion()
                || existing == null && !error && upgradeOnMissing
                || existing == null && error && upgradeOnError) {
                updates.set(toTenantKey(to), schemaBytes, -1);
            }
        }
        LOG.info("Upgrading schema for {} tenants to name:{} version:{}", updates.size(), schema.getName(), schema.getVersion());
        schemasClient().commit(Consistency.quorum, null, updates, replicateTimeoutMillis, TimeUnit.MILLISECONDS);
        return updates.size();
    }

    @Override
    public List<PartitionRange> getIngressRanges(MiruTenantId tenantId) throws Exception {
        final LinkedHashMap<MiruPartitionId, RangeMinMax> partitionLookupRange = Maps.newLinkedHashMap();
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

        List<PartitionRange> partitionRanges = Lists.newArrayList();
        for (Entry<MiruPartitionId, RangeMinMax> entry : partitionLookupRange.entrySet()) {
            MiruPartitionId partitionId = entry.getKey();
            IngressStatusTimestamps ingressStatusTimestamps = getIngressStatusTimestamps(tenantId, partitionId);
            partitionRanges.add(new PartitionRange(partitionId,
                entry.getValue(),
                ingressStatusTimestamps.destroyAfterTimestamp,
                ingressStatusTimestamps.cleanupAfterTimestamp));
        }
        return partitionRanges;
    }

    private long getIngressUpdate(MiruTenantId tenantId, MiruPartitionId partitionId, IngressType type, long defaultValue) throws Exception {
        long value = defaultValue;
        EmbeddedClient ingressClient = ingressClient();
        if (ingressClient != null) {
            byte[] clockMaxBytes = ingressClient.getValue(Consistency.quorum, null, toIngressKey(tenantId, partitionId, type));
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
        EmbeddedClient ingressClient = ingressClient();
        if (ingressClient != null) {
            ingressClient.scan(
                Collections.singletonList(new ScanRange(null, fromKey, null, toKey)),
                (prefix, key, value, timestamp, version) -> {
                    if (value != null) {
                        MiruPartitionId streamPartitionId = ingressKeyToPartitionId(key);
                        if (partitionId == null || partitionId.equals(streamPartitionId)) {
                            IngressType ingressType = ingressKeyToType(key);
                            return streamRangeLookup.stream(streamPartitionId, ingressType, FilerIO.bytesLong(value));
                        }
                    }
                    return false;
                },
                true
            );
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

    private byte[] toLastIdKey(MiruTenantId tenantId, MiruPartitionId partitionId, MiruHost host) {
        byte[] keyBytes;
        byte[] tenantBytes = tenantId.getBytes();
        if (partitionId != null) {
            if (host != null) {
                byte[] hostBytes = host.getLogicalName().getBytes(StandardCharsets.UTF_8);
                keyBytes = new byte[4 + tenantBytes.length + 4 + 4 + hostBytes.length];

                // tenant
                FilerIO.intBytes(tenantBytes.length, keyBytes, 0);
                System.arraycopy(tenantBytes, 0, keyBytes, 4, tenantBytes.length);

                // partition
                UtilLexMarshaller.intBytes(partitionId.getId(), keyBytes, 4 + tenantBytes.length);

                // host
                FilerIO.intBytes(hostBytes.length, keyBytes, 4 + tenantBytes.length + 4);
                System.arraycopy(hostBytes, 0, keyBytes, 4 + tenantBytes.length + 4 + 4, hostBytes.length);
            } else {
                keyBytes = new byte[4 + tenantBytes.length + 4];

                // tenant
                FilerIO.intBytes(tenantBytes.length, keyBytes, 0);
                System.arraycopy(tenantBytes, 0, keyBytes, 4, tenantBytes.length);

                // partition
                UtilLexMarshaller.intBytes(partitionId.getId(), keyBytes, 4 + tenantBytes.length);
            }
        } else {
            keyBytes = new byte[4 + tenantBytes.length];

            // tenant
            FilerIO.intBytes(tenantBytes.length, keyBytes, 0);
            System.arraycopy(tenantBytes, 0, keyBytes, 4, tenantBytes.length);
        }
        return keyBytes;
    }

    private MiruPartitionCoord lastIdKeyToCoord(byte[] keyBytes) {
        // tenant
        int tenantLength = FilerIO.bytesInt(keyBytes, 0);
        byte[] tenantBytes = new byte[tenantLength];
        System.arraycopy(keyBytes, 4, tenantBytes, 0, tenantLength);

        // partition
        int partitionId = FilerIO.bytesInt(keyBytes, 4 + tenantLength);

        // host
        int hostLength = FilerIO.bytesInt(keyBytes, 4 + tenantLength + 4);
        byte[] hostBytes = new byte[hostLength];
        System.arraycopy(keyBytes, 4 + tenantLength + 4 + 4, hostBytes, 0, hostLength);

        return new MiruPartitionCoord(new MiruTenantId(tenantBytes),
            MiruPartitionId.of(partitionId),
            new MiruHost(new String(hostBytes, StandardCharsets.UTF_8)));
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
