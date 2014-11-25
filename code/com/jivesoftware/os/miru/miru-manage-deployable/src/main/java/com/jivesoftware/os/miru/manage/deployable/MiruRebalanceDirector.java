package com.jivesoftware.os.miru.manage.deployable;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.Collections2;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.collect.Table;
import com.jivesoftware.os.jive.utils.base.interfaces.CallbackStream;
import com.jivesoftware.os.jive.utils.base.util.locks.StripingLocksProvider;
import com.jivesoftware.os.jive.utils.http.client.rest.RequestHelper;
import com.jivesoftware.os.jive.utils.logger.MetricLogger;
import com.jivesoftware.os.jive.utils.logger.MetricLoggerFactory;
import com.jivesoftware.os.jive.utils.ordered.id.OrderIdProvider;
import com.jivesoftware.os.miru.api.MiruHost;
import com.jivesoftware.os.miru.api.MiruPartition;
import com.jivesoftware.os.miru.api.MiruPartitionCoord;
import com.jivesoftware.os.miru.api.MiruPartitionState;
import com.jivesoftware.os.miru.api.MiruTopologyStatus;
import com.jivesoftware.os.miru.api.activity.MiruPartitionId;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.cluster.MiruClusterRegistry;
import java.awt.Color;
import java.awt.Graphics2D;
import java.awt.image.BufferedImage;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import javax.imageio.ImageIO;

/**
 *
 */
public class MiruRebalanceDirector {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    private final MiruClusterRegistry clusterRegistry;
    private final OrderIdProvider orderIdProvider;
    private final ReaderRequestHelpers readerRequestHelpers;

    private Function<MiruPartition, MiruHost> partitionToHost = new Function<MiruPartition, MiruHost>() {
        @Override
        public MiruHost apply(MiruPartition input) {
            return input.coord.host;
        }
    };
    private Function<MiruClusterRegistry.HostHeartbeat, MiruHost> heartbeatToHost = new Function<MiruClusterRegistry.HostHeartbeat, MiruHost>() {
        @Override
        public MiruHost apply(MiruClusterRegistry.HostHeartbeat input) {
            return input.host;
        }
    };

    private final AtomicReference<List<MiruTopologyStatus>> topologies = new AtomicReference<>();

    public MiruRebalanceDirector(MiruClusterRegistry clusterRegistry, OrderIdProvider orderIdProvider, ReaderRequestHelpers readerRequestHelpers) {
        this.clusterRegistry = clusterRegistry;
        this.orderIdProvider = orderIdProvider;
        this.readerRequestHelpers = readerRequestHelpers;
    }

    public void shiftTopologies(MiruHost fromHost, ShiftPredicate shiftPredicate, final SelectHostsStrategy selectHostsStrategy) throws Exception {
        LinkedHashSet<MiruClusterRegistry.HostHeartbeat> hostHeartbeats = clusterRegistry.getAllHosts();
        List<MiruHost> allHosts = Lists.newArrayList(Collections2.transform(hostHeartbeats, heartbeatToHost));

        int moved = 0;
        int skipped = 0;
        int missed = 0;
        for (MiruTenantId tenantId : clusterRegistry.getTenantsForHost(fromHost)) {
            int numberOfReplicas = clusterRegistry.getNumberOfReplicas(tenantId);
            ListMultimap<MiruPartitionState, MiruPartition> partitionsForTenant = clusterRegistry.getPartitionsForTenant(tenantId);
            MiruPartitionId currentPartitionId = findCurrentPartitionId(partitionsForTenant);
            Table<MiruTenantId, MiruPartitionId, List<MiruPartition>> replicaTable = extractPartitions(
                selectHostsStrategy.isCurrentPartitionOnly(), tenantId, partitionsForTenant, currentPartitionId);
            for (Table.Cell<MiruTenantId, MiruPartitionId, List<MiruPartition>> cell : replicaTable.cellSet()) {
                MiruPartitionId partitionId = cell.getColumnKey();
                List<MiruPartition> partitions = cell.getValue();
                Set<MiruHost> hostsWithPartition = Sets.newHashSet(Collections2.transform(partitions, partitionToHost));
                if (!hostsWithPartition.contains(fromHost)) {
                    missed++;
                    LOG.trace("Missed {} {}", tenantId, partitionId);
                    continue;
                } else if (!shiftPredicate.needsToShift(tenantId, partitionId, hostHeartbeats, partitions)) {
                    skipped++;
                    LOG.trace("Skipped {} {}", tenantId, partitionId);
                    continue;
                }
                List<MiruHost> hostsToElect = selectHostsStrategy.selectHosts(fromHost, allHosts, partitions, numberOfReplicas);
                electHosts(tenantId, partitionId, Lists.transform(partitions, partitionToHost), hostsToElect);
                moved++;
            }
        }
        LOG.info("Done shifting, moved={} skipped={} missed={}", moved, skipped, missed);
        LOG.inc("rebalance>moved", moved);
        LOG.inc("rebalance>skipped", skipped);
        LOG.inc("rebalance>missed", missed);
    }

    public void removeHost(MiruHost miruHost) throws Exception {
        RequestHelper requestHelper = readerRequestHelpers.get(Optional.of(miruHost));
        requestHelper.executeDeleteRequest("/miru/config/hosts/" + miruHost.getLogicalName() + "/" + miruHost.getPort(), String.class, null);
    }

    private void electHosts(MiruTenantId tenantId, MiruPartitionId partitionId, List<MiruHost> fromHosts, List<MiruHost> hostsToElect) throws Exception {
        LOG.debug("Elect from {} to {} for {} {}", fromHosts, hostsToElect, tenantId, partitionId);
        for (MiruHost hostToElect : hostsToElect) {
            clusterRegistry.ensurePartitionCoord(new MiruPartitionCoord(tenantId, partitionId, hostToElect));
            clusterRegistry.addToReplicaRegistry(tenantId, partitionId, Long.MAX_VALUE - orderIdProvider.nextId(), hostToElect);
        }
        LOG.inc("rebalance>elect", hostsToElect.size());
    }

    private Table<MiruTenantId, MiruPartitionId, List<MiruPartition>> extractPartitions(boolean currentPartitionOnly,
        MiruTenantId tenantId,
        ListMultimap<MiruPartitionState, MiruPartition> partitionsForTenant,
        MiruPartitionId currentPartitionId) {

        Table<MiruTenantId, MiruPartitionId, List<MiruPartition>> replicaTable = HashBasedTable.create();
        for (MiruPartition partition : partitionsForTenant.values()) {
            MiruPartitionId partitionId = partition.coord.partitionId;
            if (currentPartitionOnly && currentPartitionId != null && partitionId.compareTo(currentPartitionId) < 0) {
                continue;
            }
            List<MiruPartition> partitions = replicaTable.get(tenantId, partitionId);
            if (partitions == null) {
                partitions = Lists.newArrayList();
                replicaTable.put(tenantId, partitionId, partitions);
            }
            if (currentPartitionId == null || partitionId.compareTo(currentPartitionId) > 0) {
                currentPartitionId = partitionId;
            }
            partitions.add(partition);
        }
        return replicaTable;
    }

    private MiruPartitionId findCurrentPartitionId(ListMultimap<MiruPartitionState, MiruPartition> partitionsForTenant) {
        MiruPartitionId currentPartitionId = null;
        for (MiruPartition partition : partitionsForTenant.values()) {
            MiruPartitionId partitionId = partition.coord.partitionId;
            if (currentPartitionId == null || partitionId.compareTo(currentPartitionId) > 0) {
                currentPartitionId = partitionId;
            }
        }
        return currentPartitionId;
    }

    private static final int VISUAL_PARTITION_HEIGHT = 4;
    private static final int VISUAL_PADDING = 2;
    private static final int VISUAL_PADDING_HALVED = VISUAL_PADDING / 2;
    private static final Color[] COLORS;

    static {
        COLORS = new Color[MiruPartitionState.values().length];
        Arrays.fill(COLORS, Color.WHITE);
        COLORS[MiruPartitionState.offline.ordinal()] = Color.GRAY;
        COLORS[MiruPartitionState.bootstrap.ordinal()] = Color.BLUE;
        COLORS[MiruPartitionState.rebuilding.ordinal()] = Color.MAGENTA;
        COLORS[MiruPartitionState.online.ordinal()] = Color.GREEN;
    }

    private static class VisualizeContext {
        private final List<MiruHost> allHosts;
        private final Set<MiruHost> unhealthyHosts;
        private final List<List<MiruTenantId>> splitTenantIds;

        private VisualizeContext(List<MiruHost> allHosts, Set<MiruHost> unhealthyHosts, List<List<MiruTenantId>> splitTenantIds) {
            this.allHosts = allHosts;
            this.unhealthyHosts = unhealthyHosts;
            this.splitTenantIds = splitTenantIds;
        }
    }

    private final Cache<String, VisualizeContext> contextCache = CacheBuilder.newBuilder()
        .maximumSize(10)
        .expireAfterAccess(10, TimeUnit.MINUTES)
        .weakValues()
        .build();
    private final StripingLocksProvider<String> tokenLocks = new StripingLocksProvider<>(64);

    public void visualizeTopologies(int width, final int split, int index, String token, OutputStream out) throws Exception {
        VisualizeContext context;
        synchronized (tokenLocks.lock(token)) {
            context = contextCache.get(token, new Callable<VisualizeContext>() {
                @Override
                public VisualizeContext call() throws Exception {
                    LinkedHashSet<MiruClusterRegistry.HostHeartbeat> heartbeats = clusterRegistry.getAllHosts();
                    Set<MiruHost> unhealthyHosts = Sets.newHashSet();
                    for (MiruClusterRegistry.HostHeartbeat heartbeat : heartbeats) {
                        if (heartbeat.heartbeat < System.currentTimeMillis() - TimeUnit.MINUTES.toMillis(1)) { //TODO configure
                            unhealthyHosts.add(heartbeat.host);
                        }
                    }

                    List<MiruHost> allHosts = Lists.newArrayList(Collections2.transform(heartbeats,
                        new Function<MiruClusterRegistry.HostHeartbeat, MiruHost>() {
                            @Override
                            public MiruHost apply(MiruClusterRegistry.HostHeartbeat input) {
                                return input.host;
                            }
                        }));

                    List<MiruTenantId> allTenantIds = clusterRegistry.allTenantIds();
                    List<List<MiruTenantId>> splitTenantIds = Lists.partition(allTenantIds, Math.max(1, (allTenantIds.size() + split - 1) / split));

                    return new VisualizeContext(allHosts, unhealthyHosts, splitTenantIds);
                }
            });
        }

        final Table<MiruTenantId, MiruPartitionId, List<MiruTopologyStatus>> topologies = HashBasedTable.create();
        List<MiruTenantId> tenantIds = (index < context.splitTenantIds.size() - 1) ? context.splitTenantIds.get(index) : Collections.<MiruTenantId>emptyList();
        clusterRegistry.topologiesForTenants(tenantIds, new CallbackStream<MiruTopologyStatus>() {
            @Override
            public MiruTopologyStatus callback(MiruTopologyStatus status) throws Exception {
                if (status != null) {
                    MiruPartitionCoord coord = status.partition.coord;
                    List<MiruTopologyStatus> statuses = topologies.get(coord.tenantId, coord.partitionId);
                    if (statuses == null) {
                        statuses = Lists.newArrayList();
                        topologies.put(coord.tenantId, coord.partitionId, statuses);
                    }
                    statuses.add(status);
                }
                return status;
            }
        });

        int numHosts = context.allHosts.size();
        int numPartitions = topologies.size();
        if (numHosts == 0) {
            throw new IllegalStateException("Not enough data");
        }

        int visualHostWidth = (width - VISUAL_PADDING) / context.allHosts.size() - VISUAL_PADDING;
        BufferedImage bi = new BufferedImage(
            VISUAL_PADDING + (visualHostWidth + VISUAL_PADDING) * numHosts,
            VISUAL_PADDING + (VISUAL_PARTITION_HEIGHT + VISUAL_PADDING) * numPartitions,
            BufferedImage.TYPE_INT_RGB);
        Graphics2D ig2 = bi.createGraphics();

        int y = VISUAL_PADDING;
        for (List<MiruTopologyStatus> statuses : topologies.values()) {
            int unhealthyPartitions = 0;
            int offlinePartitions = 0;
            int onlinePartitions = 0;
            for (MiruTopologyStatus status : statuses) {
                if (context.unhealthyHosts.contains(status.partition.coord.host)) {
                    unhealthyPartitions++;
                }
                if (status.partition.info.state == MiruPartitionState.offline) {
                    offlinePartitions++;
                }
                if (status.partition.info.state == MiruPartitionState.online) {
                    onlinePartitions++;
                }
            }
            Color unhealthyColor = null;
            if (unhealthyPartitions > 0) {
                int numReplicas = statuses.size();
                float unhealthyPct = (float) unhealthyPartitions / (float) numReplicas;
                if (unhealthyPct < 0.33f) {
                    unhealthyColor = Color.YELLOW;
                } else if (unhealthyPct < 0.67f) {
                    unhealthyColor = Color.ORANGE;
                } else {
                    unhealthyColor = Color.RED;
                }
            }

            if (offlinePartitions < statuses.size() && onlinePartitions == 0) {
                // partition appears to be awake, but nothing is online, so paint the background
                ig2.setColor(new Color(64, 0, 0));
                ig2.fillRect(VISUAL_PADDING_HALVED,
                    y - VISUAL_PADDING_HALVED,
                    (visualHostWidth + VISUAL_PADDING) * numHosts,
                    VISUAL_PARTITION_HEIGHT + VISUAL_PADDING);
            }

            for (MiruTopologyStatus status : statuses) {
                int x = VISUAL_PADDING + context.allHosts.indexOf(status.partition.coord.host) * (visualHostWidth + VISUAL_PADDING);
                if (context.unhealthyHosts.contains(status.partition.coord.host)) {
                    ig2.setColor(unhealthyColor);
                } else {
                    ig2.setColor(COLORS[status.partition.info.state.ordinal()]);
                }
                ig2.fillRect(x, y, visualHostWidth, VISUAL_PARTITION_HEIGHT);
            }

            y += VISUAL_PARTITION_HEIGHT + VISUAL_PADDING;
        }

        ImageIO.write(bi, "PNG", out);
    }
}
