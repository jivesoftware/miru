package com.jivesoftware.os.miru.cluster.amza;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.base.Predicate;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.MinMaxPriorityQueue;
import com.google.common.collect.Multimaps;
import com.google.common.collect.SetMultimap;
import com.jivesoftware.os.amza.service.AmzaRegion;
import com.jivesoftware.os.amza.service.AmzaService;
import com.jivesoftware.os.amza.shared.PrimaryIndexDescriptor;
import com.jivesoftware.os.amza.shared.RegionName;
import com.jivesoftware.os.amza.shared.RegionProperties;
import com.jivesoftware.os.amza.shared.RingHost;
import com.jivesoftware.os.amza.shared.Scan;
import com.jivesoftware.os.amza.shared.WALKey;
import com.jivesoftware.os.amza.shared.WALStorageDescriptor;
import com.jivesoftware.os.amza.shared.WALValue;
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
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author jonathan.colt
 */
public class AmzaClusterRegistry implements MiruClusterRegistry {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    private static final byte[] EMPTY_BYTES = new byte[0];
    private static final String AMZA_RING_NAME = "clusterRegistry";

    private final MiruHostMarshaller hostMarshaller = new MiruHostMarshaller();
    private final MiruTopologyColumnValueMarshaller topologyColumnValueMarshaller = new MiruTopologyColumnValueMarshaller();
    private final TypeMarshaller<MiruSchema> schemaMarshaller;

    private final AmzaService amzaService;
    private final int defaultNumberOfReplicas;
    private final long defaultTopologyIsStaleAfterMillis;
    private final long defaultTopologyIsIdleAfterMillis;
    private final int replicationFactor;
    private final int takeFromFactor;
    private final WALStorageDescriptor amzaStorageDescriptor;

    private final AtomicBoolean ringInitialized = new AtomicBoolean(false);

    public AmzaClusterRegistry(AmzaService amzaService,
        TypeMarshaller<MiruSchema> schemaMarshaller,
        int defaultNumberOfReplicas,
        long defaultTopologyIsStaleAfterMillis,
        long defaultTopologyIsIdleAfterMillis,
        int replicationFactor,
        int takeFromFactor) throws Exception {
        this.amzaService = amzaService;
        this.schemaMarshaller = schemaMarshaller;
        this.defaultNumberOfReplicas = defaultNumberOfReplicas;
        this.defaultTopologyIsStaleAfterMillis = defaultTopologyIsStaleAfterMillis;
        this.defaultTopologyIsIdleAfterMillis = defaultTopologyIsIdleAfterMillis;
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
        createRegionIfAbsent("hosts").scan(new Scan<WALValue>() {

            @Override
            public boolean row(long transactionId, WALKey key, WALValue value) throws Exception {
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

    static class TenantAndPartition {

        public final byte[] tenantBytes;
        public final int partitionId;

        public TenantAndPartition(byte[] miruTenantId, int miruPartitionId) {
            this.tenantBytes = miruTenantId;
            this.partitionId = miruPartitionId;
        }

    }

    private AmzaRegion createRegionIfAbsent(String regionName) throws Exception {
        if (!ringInitialized.get()) {
            List<RingHost> ring = amzaService.getAmzaRing().getRing(AMZA_RING_NAME);
            if (ring.isEmpty()) {
                amzaService.getAmzaRing().buildRandomSubRing(AMZA_RING_NAME, amzaService.getAmzaRing().getRing("system").size());
            }
            ringInitialized.set(true);
        }

        return amzaService.createRegionIfAbsent(new RegionName(false, AMZA_RING_NAME, regionName),
            new RegionProperties(amzaStorageDescriptor, replicationFactor, takeFromFactor, false)
        );
    }

    @Override
    public void updateTopologies(MiruHost host, List<TopologyUpdate> topologyUpdates) throws Exception {

        final AmzaRegion topology = createRegionIfAbsent("host-" + host.toStringForm() + "-partition-info");

        ImmutableMap<WALKey, TopologyUpdate> keyToUpdateMap = Maps.uniqueIndex(topologyUpdates, new Function<TopologyUpdate, WALKey>() {
            @Override
            public WALKey apply(TopologyUpdate topologyUpdate) {
                return topologyKey(topologyUpdate.coord.tenantId, topologyUpdate.coord.partitionId);
            }
        });

        Map<WALKey, byte[]> keyToBytesMap = Maps.transformValues(keyToUpdateMap, new Function<TopologyUpdate, byte[]>() {
            @Override
            public byte[] apply(TopologyUpdate topologyUpdate) {
                try {
                    MiruPartitionCoord coord = topologyUpdate.coord;
                    Optional<MiruPartitionCoordInfo> optionalInfo = topologyUpdate.optionalInfo;
                    Optional<Long> refreshTimestamp = topologyUpdate.refreshTimestamp;
                    WALKey key = topologyKey(coord.tenantId, coord.partitionId);

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

                    MiruTopologyColumnValue value = new MiruTopologyColumnValue(coordInfo.state, coordInfo.storage, timestamp);
                    LOG.debug("Updating {} to {} at {}", new Object[] { coord, coordInfo, refreshTimestamp });
                    return topologyColumnValueMarshaller.toBytes(value);
                } catch (Exception e) {
                    throw new RuntimeException("Failed to apply topology update", e);
                }
            }
        });

        topology.set(keyToBytesMap.entrySet());

        markTenantTopologyUpdated(FluentIterable.from(topologyUpdates)
            .filter(hasUpdatedInfo)
            .transform(extractUpdatedTenantId)
            .toSet());
    }

    private static final Predicate<TopologyUpdate> hasUpdatedInfo = new Predicate<TopologyUpdate>() {
        @Override
        public boolean apply(TopologyUpdate input) {
            return input.optionalInfo.isPresent();
        }
    };

    private static final Function<TopologyUpdate, MiruTenantId> extractUpdatedTenantId = new Function<TopologyUpdate, MiruTenantId>() {
        @Override
        public MiruTenantId apply(TopologyUpdate input) {
            return input.coord.tenantId;
        }
    };

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
    public List<MiruTenantTopologyUpdate> getTopologyUpdatesForHost(MiruHost host, long sinceTimestamp) throws Exception {
        final List<MiruTenantTopologyUpdate> updates = Lists.newArrayList();
        final long acceptableTimestampId = amzaService.getTimestamp(sinceTimestamp, TimeUnit.MINUTES.toMillis(1));

        AmzaRegion topology = createRegionIfAbsent("host-" + host.toStringForm() + "-topology-updates");
        topology.scan(new Scan<WALValue>() {
            @Override
            public boolean row(long transactionId, WALKey key, WALValue value) throws Exception {
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

        AmzaRegion topology = createRegionIfAbsent("host-" + coord.host.toStringForm() + "-partition-info");
        WALKey key = topologyKey(coord.tenantId, coord.partitionId);
        if (topology.get(key) == null) { // TODO don't have a set if absent. This is a little racy
            MiruTopologyColumnValue update = new MiruTopologyColumnValue(MiruPartitionState.offline, MiruBackingStorage.memory, 0);
            topology.set(key, topologyColumnValueMarshaller.toBytes(update));
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
        topology.scan(new Scan<WALValue>() {

            @Override
            public boolean row(long transactionId, WALKey key, WALValue value) throws Exception {
                TenantAndPartition topologyKey = topologyKey(key);
                tenants.add(new MiruTenantId(topologyKey.tenantBytes));
                return true;
            }
        });
        return tenants;
    }

    @Override
    public void removeTenantPartionReplicaSet(final MiruTenantId tenantId, final MiruPartitionId partitionId) throws Exception {
        createRegionIfAbsent("hosts").scan(new Scan<WALValue>() {

            @Override
            public boolean row(long transactionId, WALKey key, WALValue value) throws Exception {
                if (!value.getTombstoned()) { // paranoid
                    MiruHost host = hostMarshaller.fromBytes(key.getKey());
                    AmzaRegion topologyReg = createRegionIfAbsent("host-" + host.toStringForm() + "-partition-registry");
                    AmzaRegion topologyInfo = createRegionIfAbsent("host-" + host.toStringForm() + "-partition-info");
                    WALKey topologyKey = topologyKey(tenantId, partitionId);
                    topologyReg.remove(topologyKey);
                    topologyInfo.remove(topologyKey);
                }
                return true;
            }
        });
        markTenantTopologyUpdated(Collections.singleton(tenantId));
    }

    @Override
    public List<MiruPartition> getPartitionsForTenant(MiruTenantId tenantId) throws Exception {
        ConcurrentSkipListMap<Integer, MinMaxPriorityQueue<HostAndTimestamp>> paritionIdToLatest = tenantsLatestTopology(tenantId);

        List<MiruPartition> partitions = new ArrayList<>();
        for (int partitionId : paritionIdToLatest.keySet()) {
            MiruPartitionId miruPartitionId = MiruPartitionId.of(partitionId);
            MinMaxPriorityQueue<HostAndTimestamp> got = paritionIdToLatest.get(partitionId);
            for (HostAndTimestamp hat : got) {
                AmzaRegion topologyInfo = createRegionIfAbsent("host-" + hat.host.toStringForm() + "-partition-info");
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
        final WALKey from = new WALKey(tenantId.getBytes());
        final WALKey to = new WALKey(prefixUpperExclusive(tenantId.getBytes()));
        final ConcurrentSkipListMap<Integer, MinMaxPriorityQueue<HostAndTimestamp>> partitionIdToLatest = new ConcurrentSkipListMap<>();
        createRegionIfAbsent("hosts").scan(new Scan<WALValue>() {

            @Override
            public boolean row(long transactionId, WALKey key, WALValue value) throws Exception {
                if (!value.getTombstoned()) { // paranoid
                    final MiruHost host = hostMarshaller.fromBytes(key.getKey());
                    AmzaRegion topologyReg = createRegionIfAbsent("host-" + host.toStringForm() + "-partition-registry");
                    topologyReg.rangeScan(from, to, new Scan<WALValue>() {

                        @Override
                        public boolean row(long transactionId, WALKey key, WALValue value) throws Exception {
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
                    AmzaRegion topologyInfo = createRegionIfAbsent("host-" + hat.host.toStringForm() + "-partition-info");
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
                AmzaRegion topologyInfo = createRegionIfAbsent("host-" + hat.host.toStringForm() + "-partition-info");
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
                    AmzaRegion topologyInfo = createRegionIfAbsent("host-" + hat.host.toStringForm() + "-partition-info");
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
                    AmzaRegion topologyInfo = createRegionIfAbsent("host-" + hat.host.toStringForm() + "-partition-info");
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
        AmzaRegion topologyInfo = createRegionIfAbsent("host-" + coord.host.toStringForm() + "-partition-info");
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
