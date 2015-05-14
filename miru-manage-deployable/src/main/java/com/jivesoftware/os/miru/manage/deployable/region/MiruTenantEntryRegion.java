package com.jivesoftware.os.miru.manage.deployable.region;

import com.google.common.collect.Maps;
import com.jivesoftware.os.miru.api.MiruPartition;
import com.jivesoftware.os.miru.api.MiruPartitionState;
import com.jivesoftware.os.miru.api.MiruTopologyStatus;
import com.jivesoftware.os.miru.api.activity.MiruPartitionId;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.api.topology.MiruTenantConfig;
import com.jivesoftware.os.miru.api.wal.MiruActivityWALStatus;
import com.jivesoftware.os.miru.api.wal.MiruWALClient;
import com.jivesoftware.os.miru.cluster.MiruClusterRegistry;
import com.jivesoftware.os.miru.cluster.MiruTenantConfigFields;
import com.jivesoftware.os.miru.manage.deployable.MiruSoyRenderer;
import com.jivesoftware.os.miru.manage.deployable.region.bean.PartitionBean;
import com.jivesoftware.os.miru.manage.deployable.region.bean.PartitionCoordBean;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.concurrent.TimeUnit;

/**
 *
 */
public class MiruTenantEntryRegion implements MiruRegion<MiruTenantId> {

    private static final MetricLogger log = MetricLoggerFactory.getLogger();

    private final String template;
    private final MiruSoyRenderer renderer;
    private final MiruClusterRegistry clusterRegistry;
    private final MiruWALClient<?, ?> miruWALClient;

    public MiruTenantEntryRegion(String template,
        MiruSoyRenderer renderer,
        MiruClusterRegistry clusterRegistry,
        MiruWALClient miruWALClient) {

        this.template = template;
        this.renderer = renderer;
        this.clusterRegistry = clusterRegistry;
        this.miruWALClient = miruWALClient;
    }

    @Override
    public String render(MiruTenantId tenant) {
        Map<String, Object> data = Maps.newHashMap();
        try {
            MiruTenantConfig config = clusterRegistry.getTenantConfig(tenant);
            data.put("config", true);
            data.put("configNumberOfReplicas", String.valueOf(config.getLong(MiruTenantConfigFields.number_of_replicas.name(), -1)));
            data.put("configTopologyIsStaleAfterMillis", String.valueOf(config.getLong(MiruTenantConfigFields.topology_is_stale_after_millis.name(), -1)));
        } catch (Exception e) {
            log.error("Failed to get config for tenant: " + tenant);
            data.put("config", false);
        }

        SortedMap<MiruPartitionId, PartitionBean> partitionsMap = Maps.newTreeMap();
        try {
            List<MiruTopologyStatus> statusForTenant = clusterRegistry.getTopologyStatusForTenant(tenant);

            MiruPartitionId latestPartitionId = miruWALClient.getLargestPartitionIdAcrossAllWriters(tenant);

            if (latestPartitionId != null) {
                List<MiruPartitionId> partitionIds = new ArrayList<>();
                for (MiruPartitionId latest = latestPartitionId; latest != null; latest = latest.prev()) {
                    partitionIds.add(latest);
                }
                for (MiruActivityWALStatus status : miruWALClient.getPartitionStatus(tenant, partitionIds)) {
                    if (status != null) {
                        partitionsMap.put(status.partitionId,
                            new PartitionBean(status.partitionId.getId(), status.count, status.begins.size(), status.ends.size()));
                    }
                }
            }

            for (MiruTopologyStatus topologyStatus : statusForTenant) {
                MiruPartition partition = topologyStatus.partition;
                MiruPartitionId partitionId = partition.coord.partitionId;
                PartitionBean partitionBean = getPartitionBean(tenant, partitionsMap, partitionId);
                MiruPartitionState state = partition.info.state;
                String lastIngress = timeAgo(System.currentTimeMillis() - topologyStatus.lastIngressTimestamp);
                String lastQuery = timeAgo(System.currentTimeMillis() - topologyStatus.lastQueryTimestamp);
                PartitionCoordBean partitionCoordBean = new PartitionCoordBean(partition.coord, partition.info.storage, lastIngress, lastQuery);
                if (state == MiruPartitionState.online) {
                    partitionBean.getOnline().add(partitionCoordBean);
                } else if (state == MiruPartitionState.rebuilding) {
                    partitionBean.getRebuilding().add(partitionCoordBean);
                } else if (state == MiruPartitionState.bootstrap) {
                    partitionBean.getBootstrap().add(partitionCoordBean);
                } else if (state == MiruPartitionState.offline) {
                    partitionBean.getOffline().add(partitionCoordBean);
                }
            }

            Collection<MiruWALClient.MiruLookupRange> lookupRanges = miruWALClient.lookupRanges(tenant);
            if (lookupRanges != null) {
                for (MiruWALClient.MiruLookupRange lookupRange : lookupRanges) {
                    MiruPartitionId partitionId = MiruPartitionId.of(lookupRange.partitionId);
                    PartitionBean partitionBean = getPartitionBean(tenant, partitionsMap, partitionId);
                    partitionBean.setMinClock(String.valueOf(lookupRange.minClock));
                    partitionBean.setMaxClock(String.valueOf(lookupRange.maxClock));
                    partitionBean.setMinOrderId(String.valueOf(lookupRange.minOrderId));
                    partitionBean.setMaxOrderId(String.valueOf(lookupRange.maxOrderId));
                }
            }
        } catch (Exception e) {
            log.error("Unable to get partitions for tenant: " + tenant);
        }

        data.put("tenant", tenant.toString());
        data.put("partitions", partitionsMap.values());

        return renderer.render(template, data);
    }

    private PartitionBean getPartitionBean(MiruTenantId tenant,
        SortedMap<MiruPartitionId, PartitionBean> partitionsMap,
        MiruPartitionId partitionId) throws Exception {
        PartitionBean partitionBean = partitionsMap.get(partitionId);
        if (partitionBean == null) {
            List<MiruActivityWALStatus> partitionStatus = miruWALClient.getPartitionStatus(tenant, Collections.singletonList(partitionId));
            for (MiruActivityWALStatus status : partitionStatus) {
                if (status != null && status.partitionId.equals(partitionId)) {
                    partitionBean = new PartitionBean(status.partitionId.getId(), status.count, status.begins.size(), status.ends.size());
                    break;
                }
            }
            if (partitionBean == null) {
                partitionBean = new PartitionBean(partitionId.getId(), -1, -1, -1);
            }
            partitionsMap.put(partitionId, partitionBean);
        }
        return partitionBean;
    }

    private static String timeAgo(long millis) {
        String suffix;
        if (millis >= 0) {
            suffix = "ago";
        } else {
            suffix = "from now";
            millis = Math.abs(millis);
        }

        final long hr = TimeUnit.MILLISECONDS.toHours(millis);
        final long min = TimeUnit.MILLISECONDS.toMinutes(millis - TimeUnit.HOURS.toMillis(hr));
        final long sec = TimeUnit.MILLISECONDS.toSeconds(millis - TimeUnit.HOURS.toMillis(hr) - TimeUnit.MINUTES.toMillis(min));
        final long ms = TimeUnit.MILLISECONDS.toMillis(millis - TimeUnit.HOURS.toMillis(hr) - TimeUnit.MINUTES.toMillis(min) - TimeUnit.SECONDS.toMillis(sec));
        return String.format("%02d:%02d:%02d.%03d " + suffix, hr, min, sec, ms);
    }

}
