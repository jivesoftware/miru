package com.jivesoftware.os.miru.manage.deployable.balancer;

import com.google.common.base.Charsets;
import com.google.common.base.Objects;
import com.google.common.base.Optional;
import com.google.common.base.Splitter;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Table;
import com.jivesoftware.os.jive.utils.ordered.id.OrderIdProvider;
import com.jivesoftware.os.miru.api.MiruHost;
import com.jivesoftware.os.miru.api.MiruHostProvider;
import com.jivesoftware.os.miru.api.MiruHostSelectiveStrategy;
import com.jivesoftware.os.miru.api.MiruPartition;
import com.jivesoftware.os.miru.api.MiruReader;
import com.jivesoftware.os.miru.api.activity.MiruPartitionId;
import com.jivesoftware.os.miru.api.activity.TenantAndPartition;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.api.topology.HostHeartbeat;
import com.jivesoftware.os.miru.api.wal.MiruWALClient;
import com.jivesoftware.os.miru.cluster.MiruClusterRegistry;
import com.jivesoftware.os.miru.cluster.MiruReplicaSet;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import com.jivesoftware.os.routing.bird.http.client.HttpClientException;
import com.jivesoftware.os.routing.bird.http.client.HttpResponse;
import com.jivesoftware.os.routing.bird.http.client.HttpResponseMapper;
import com.jivesoftware.os.routing.bird.http.client.TenantAwareHttpClient;
import com.jivesoftware.os.routing.bird.shared.ClientCall.ClientResponse;
import com.jivesoftware.os.routing.bird.shared.ConnectionDescriptor;
import com.jivesoftware.os.routing.bird.shared.ConnectionDescriptors;
import com.jivesoftware.os.routing.bird.shared.HostPort;
import com.jivesoftware.os.routing.bird.shared.InstanceDescriptor;
import com.jivesoftware.os.routing.bird.shared.TenantsServiceConnectionDescriptorProvider;
import difflib.Delta;
import difflib.DiffUtils;
import difflib.Patch;
import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

/**
 *
 */
public class MiruRebalanceDirector {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    private final MiruClusterRegistry clusterRegistry;
    private final MiruWALClient<?, ?> miruWALClient;
    private final OrderIdProvider orderIdProvider;
    private final TenantAwareHttpClient<String> readerClient;
    private final HttpResponseMapper responseMapper;
    private final TenantsServiceConnectionDescriptorProvider<String> readerConnectionDescriptorsProvider;

    public MiruRebalanceDirector(MiruClusterRegistry clusterRegistry,
        MiruWALClient miruWALClient,
        OrderIdProvider orderIdProvider,
        TenantAwareHttpClient<String> readerClient,
        HttpResponseMapper responseMapper,
        TenantsServiceConnectionDescriptorProvider<String> readerConnectionDescriptorsProvider) {
        this.clusterRegistry = clusterRegistry;
        this.miruWALClient = miruWALClient;
        this.orderIdProvider = orderIdProvider;
        this.readerClient = readerClient;
        this.responseMapper = responseMapper;
        this.readerConnectionDescriptorsProvider = readerConnectionDescriptorsProvider;
    }

    private Map<MiruHost, MiruHost> buildCoercionMap(boolean forceInstance) {
        Map<MiruHost, MiruHost> coercionMap = Maps.newHashMap();
        if (forceInstance) {
            ConnectionDescriptors connectionDescriptors = readerConnectionDescriptorsProvider.getConnections("");
            List<ConnectionDescriptor> descriptors = connectionDescriptors.getConnectionDescriptors();
            for (ConnectionDescriptor descriptor : descriptors) {
                InstanceDescriptor instanceDescriptor = descriptor.getInstanceDescriptor();
                HostPort hostPort = descriptor.getHostPort();
                coercionMap.put(MiruHostProvider.fromHostPort(hostPort.getHost(), hostPort.getPort()),
                    MiruHostProvider.fromInstance(instanceDescriptor.instanceName, instanceDescriptor.instanceKey));
            }
        }
        return coercionMap;
    }

    public void exportTopology(OutputStream os, boolean forceInstance) throws IOException {
        try {
            Map<MiruHost, MiruHost> coercionMap = buildCoercionMap(forceInstance);
            BufferedOutputStream buf = new BufferedOutputStream(os);
            List<MiruTenantId> tenantIds = miruWALClient.getAllTenantIds();
            AtomicLong exported = new AtomicLong(0);
            clusterRegistry.topologiesForTenants(tenantIds, status -> {
                if (status != null) {
                    MiruHost host = Objects.firstNonNull(coercionMap.get(status.partition.coord.host), status.partition.coord.host);
                    buf.write(status.partition.coord.tenantId.getBytes());
                    buf.write(',');
                    buf.write(String.valueOf(status.partition.coord.partitionId.getId()).getBytes(Charsets.US_ASCII));
                    buf.write(',');
                    buf.write(host.getLogicalName().getBytes(Charsets.US_ASCII));
                    buf.write('\n');
                    exported.incrementAndGet();
                }
                return status;
            });
            buf.flush();
            LOG.info("Exported {} topologies", exported.get());
        } catch (Exception x) {
            throw new RuntimeException(x);
        }
    }

    public void importTopology(InputStream in, boolean forceInstance) throws Exception {
        Map<MiruHost, MiruHost> coercionMap = buildCoercionMap(forceInstance);
        Splitter splitter = Splitter.on(',');
        BufferedReader reader = new BufferedReader(new InputStreamReader(in));
        ListMultimap<MiruHost, TenantAndPartition> elections = ArrayListMultimap.create();
        String line;
        while ((line = reader.readLine()) != null) {
            Iterator<String> split = splitter.split(line).iterator();
            MiruTenantId tenantId = split.hasNext() ? new MiruTenantId(split.next().getBytes(Charsets.US_ASCII)) : null;
            MiruPartitionId partitionId = split.hasNext() ? MiruPartitionId.of(Integer.parseInt(split.next())) : null;
            MiruHost host = split.hasNext() ? new MiruHost(split.next()) : null;
            if (tenantId != null && partitionId != null && host != null) {
                host = Objects.firstNonNull(coercionMap.get(host), host);
                elections.put(host, new TenantAndPartition(tenantId, partitionId));
            }
        }
        clusterRegistry.ensurePartitionCoords(elections);
        clusterRegistry.addToReplicaRegistry(elections, Long.MAX_VALUE - orderIdProvider.nextId());
        LOG.info("Imported {} topologies", elections.size());
    }

    public void shiftTopologies(Optional<MiruHost> fromHost, ShiftPredicate shiftPredicate, final SelectHostsStrategy selectHostsStrategy) throws Exception {
        LinkedHashSet<HostHeartbeat> hostHeartbeats = clusterRegistry.getAllHosts();
        List<MiruHost> allHosts = hostHeartbeats.stream().map(input -> input.host).collect(Collectors.toList());

        int moved = 0;
        int skipped = 0;
        int missed = 0;
        List<MiruTenantId> tenantIds;
        if (fromHost.isPresent()) {
            tenantIds = clusterRegistry.getTenantsForHost(fromHost.get());
        } else {
            tenantIds = miruWALClient.getAllTenantIds();
        }

        Table<MiruTenantId, MiruPartitionId, Shift> shiftTable = HashBasedTable.create();
        for (MiruTenantId tenantId : tenantIds) {
            int numberOfReplicas = clusterRegistry.getNumberOfReplicas(tenantId);
            List<MiruPartition> partitionsForTenant = clusterRegistry.getPartitionsForTenant(tenantId);
            MiruPartitionId currentPartitionId = miruWALClient.getLargestPartitionId(tenantId);
            Table<MiruTenantId, MiruPartitionId, List<MiruPartition>> replicaTable = extractPartitions(
                selectHostsStrategy.isCurrentPartitionOnly(), tenantId, partitionsForTenant, currentPartitionId);
            for (Table.Cell<MiruTenantId, MiruPartitionId, List<MiruPartition>> cell : replicaTable.cellSet()) {
                MiruPartitionId partitionId = cell.getColumnKey();
                List<MiruPartition> partitions = cell.getValue();
                Set<MiruHost> hostsWithPartition = partitions.stream().map(input -> input.coord.host).collect(Collectors.toSet());
                if (fromHost.isPresent() && !hostsWithPartition.contains(fromHost.get())) {
                    missed++;
                    LOG.trace("Missed {} {}", tenantId, partitionId);
                    continue;
                } else if (!shiftPredicate.needsToShift(tenantId, partitionId, hostHeartbeats, partitions)) {
                    skipped++;
                    LOG.trace("Skipped {} {}", tenantId, partitionId);
                    continue;
                }

                MiruHost pivotHost;
                if (fromHost.isPresent()) {
                    pivotHost = fromHost.get();
                } else if (partitions.isEmpty()) {
                    pivotHost = allHosts.get(Math.abs(Objects.hashCode(tenantId, partitionId) % allHosts.size()));
                } else {
                    pivotHost = partitions.get(0).coord.host;
                }
                List<MiruHost> hostsToElect = selectHostsStrategy.selectHosts(pivotHost, allHosts, partitions, numberOfReplicas);
                shiftTable.put(tenantId, partitionId, new Shift(Lists.transform(partitions, input -> input.coord.host), hostsToElect));
                moved++;
            }
        }
        electHosts(shiftTable);
        LOG.info("Done shifting, moved={} skipped={} missed={}", moved, skipped, missed);
        LOG.inc("rebalance>moved", moved);
        LOG.inc("rebalance>skipped", skipped);
        LOG.inc("rebalance>missed", missed);
    }

    public void debugTenant(MiruTenantId tenantId, StringBuilder stringBuilder) throws Exception {
        clusterRegistry.debugTenant(tenantId, stringBuilder);
    }

    public void debugTenantPartition(MiruTenantId tenantId, MiruPartitionId partitionId, StringBuilder stringBuilder) throws Exception {
        clusterRegistry.debugTenantPartition(tenantId, partitionId, stringBuilder);
    }

    public String diffTenantPartition(MiruTenantId tenantId, MiruPartitionId partitionId) throws Exception {
        MiruReplicaSet replicaSet = clusterRegistry.getReplicaSet(tenantId, partitionId);
        List<MiruHost> hosts = Lists.newArrayList(replicaSet.getHostsWithReplica());
        List<List<String>> diffables = Lists.newArrayList();
        for (MiruHost miruHost : hosts) {
            List<String> diffable = null;
            try {
                MiruHostSelectiveStrategy strategy = new MiruHostSelectiveStrategy(new MiruHost[] { miruHost });
                diffable = readerClient.call("", strategy, "diffTenantPartition",
                    httpClient -> {
                        String endpoint = MiruReader.QUERY_SERVICE_ENDPOINT_PREFIX + MiruReader.TIMESTAMPS_ENDPOINT + "/" + tenantId + "/" + partitionId;
                        HttpResponse response = httpClient.get(endpoint, null);
                        if (responseMapper.isSuccessStatusCode(response.getStatusCode())) {
                            @SuppressWarnings("unchecked")
                            List<String> result = responseMapper.extractResultFromResponse(response, List.class, new Class[] { String.class }, null);
                            return new ClientResponse<>(result, true);
                        } else {
                            return new ClientResponse<>(null, false);
                        }
                    });
            } catch (HttpClientException e) {
                LOG.error("Failed to diff {}", new Object[] { miruHost }, e);
            }
            diffables.add(diffable);
        }

        StringBuilder buf = new StringBuilder();
        /*
        CHANGED 3, 5 <<<
        foo bra
        -----
        foo bar
        >>>
        */
        for (int i = 0; i < diffables.size(); i++) {
            for (int j = i + 1; j < diffables.size(); j++) {
                MiruHost hostI = hosts.get(i);
                MiruHost hostJ = hosts.get(j);
                buf.append("==================================== ")
                    .append(hostI)
                    .append(" to ")
                    .append(hostJ)
                    .append(" ====================================\n");
                List<String> diffI = diffables.get(i);
                List<String> diffJ = diffables.get(j);
                if (diffI == null || diffJ == null) {
                    buf.append("Nulls ").append(diffI == null).append(' ').append(diffJ == null).append('\n');
                } else {
                    Patch patch = DiffUtils.diff(diffI, diffJ);
                    List<Delta> deltas = patch.getDeltas();
                    for (Delta delta : deltas) {
                        buf.append(delta.getType().name())
                            .append(' ')
                            .append(delta.getOriginal().getPosition())
                            .append(", ")
                            .append(delta.getRevised().getPosition())
                            .append(" <<<")
                            .append('\n');
                        for (String line : (List<String>) delta.getOriginal().getLines()) {
                            buf.append(line).append('\n');
                        }
                        buf.append("-----\n");
                        for (String line : (List<String>) delta.getRevised().getLines()) {
                            buf.append(line).append('\n');
                        }
                        buf.append(">>>\n");
                    }
                }
                buf.append("\n\n");
            }
        }
        return buf.toString();
    }

    private static class Shift {

        private final List<MiruHost> fromHosts;
        private final List<MiruHost> hostsToElect;

        public Shift(List<MiruHost> fromHosts, List<MiruHost> hostsToElect) {
            this.fromHosts = fromHosts;
            this.hostsToElect = hostsToElect;
        }
    }

    private void electHosts(Table<MiruTenantId, MiruPartitionId, Shift> shiftTable) throws Exception {
        ListMultimap<MiruHost, TenantAndPartition> elections = ArrayListMultimap.create();
        int elected = 0;
        for (Table.Cell<MiruTenantId, MiruPartitionId, Shift> cell : shiftTable.cellSet()) {
            MiruTenantId tenantId = cell.getRowKey();
            MiruPartitionId partitionId = cell.getColumnKey();
            Shift shift = cell.getValue();
            LOG.debug("Elect from {} to {} for {} {}", shift.fromHosts, shift.hostsToElect, tenantId, partitionId);
            for (MiruHost host : shift.hostsToElect) {
                elections.put(host, new TenantAndPartition(tenantId, partitionId));
            }
            elected += shift.hostsToElect.size();
        }

        clusterRegistry.ensurePartitionCoords(elections);
        clusterRegistry.addToReplicaRegistry(elections, Long.MAX_VALUE - orderIdProvider.nextId());
        LOG.inc("rebalance>elect", elected);
    }

    private Table<MiruTenantId, MiruPartitionId, List<MiruPartition>> extractPartitions(boolean currentPartitionOnly,
        MiruTenantId tenantId,
        List<MiruPartition> partitionsForTenant,
        final MiruPartitionId currentPartitionId) {

        Table<MiruTenantId, MiruPartitionId, List<MiruPartition>> replicaTable = HashBasedTable.create();

        if (currentPartitionOnly) {
            replicaTable.put(tenantId, currentPartitionId, Lists.newArrayList());
        } else {
            for (MiruPartitionId partitionId = currentPartitionId; partitionId != null; partitionId = partitionId.prev()) {
                replicaTable.put(tenantId, partitionId, Lists.newArrayList());
            }
        }

        for (MiruPartition partition : partitionsForTenant) {
            MiruPartitionId partitionId = partition.coord.partitionId;
            List<MiruPartition> partitions = replicaTable.get(tenantId, partitionId);
            if (partitions != null) {
                partitions.add(partition);
            }
        }

        return replicaTable;
    }

    public void rebuildTenantPartition(MiruHost miruHost, MiruTenantId tenantId, MiruPartitionId partitionId) throws Exception {
        String result = readerClient.call("", new MiruHostSelectiveStrategy(new MiruHost[] { miruHost }), "prioritizeRebuild", httpClient -> {
            HttpResponse response = httpClient.postJson("/miru/config/rebuild/prioritize/" + tenantId + "/" + partitionId, "{}", null);
            return new ClientResponse<>(responseMapper.extractResultFromResponse(response, String.class, null), true);
        });
        if (result == null) {
            throw new RuntimeException("Failed to rebuild tenant " + tenantId + " partition " + partitionId);
        }
    }

}
