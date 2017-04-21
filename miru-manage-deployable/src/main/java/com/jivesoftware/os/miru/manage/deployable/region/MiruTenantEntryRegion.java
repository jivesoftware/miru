package com.jivesoftware.os.miru.manage.deployable.region;

import com.google.common.collect.Maps;
import com.jivesoftware.os.jive.utils.ordered.id.SnowflakeIdPacker;
import com.jivesoftware.os.miru.api.MiruPartition;
import com.jivesoftware.os.miru.api.MiruPartitionState;
import com.jivesoftware.os.miru.api.activity.MiruPartitionId;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.api.topology.MiruClusterClient.PartitionRange;
import com.jivesoftware.os.miru.api.topology.MiruTenantConfig;
import com.jivesoftware.os.miru.api.topology.MiruTopologyStatus;
import com.jivesoftware.os.miru.api.topology.RangeMinMax;
import com.jivesoftware.os.miru.api.wal.MiruActivityWALStatus;
import com.jivesoftware.os.miru.api.wal.MiruWALClient;
import com.jivesoftware.os.miru.cluster.MiruClusterRegistry;
import com.jivesoftware.os.miru.cluster.MiruTenantConfigFields;
import com.jivesoftware.os.miru.manage.deployable.region.bean.PartitionBean;
import com.jivesoftware.os.miru.manage.deployable.region.bean.PartitionCoordBean;
import com.jivesoftware.os.miru.ui.MiruRegion;
import com.jivesoftware.os.miru.ui.MiruSoyRenderer;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
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
    private final SnowflakeIdPacker idPacker = new SnowflakeIdPacker();

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

            for (MiruTopologyStatus topologyStatus : statusForTenant) {
                MiruPartition partition = topologyStatus.partition;
                MiruPartitionId partitionId = partition.coord.partitionId;
                partitionsMap.compute(partitionId, (key, partitionBean) -> {
                    if (partitionBean == null) {
                        partitionBean = new PartitionBean(partitionId.getId());
                    }
                    MiruPartitionState state = partition.info.state;
                    String lastIngress = timeAgo(System.currentTimeMillis() - topologyStatus.lastIngressTimestamp);
                    String lastQuery = timeAgo(System.currentTimeMillis() - topologyStatus.lastQueryTimestamp);
                    String lastTimestamp = String.valueOf(topologyStatus.lastTimestamp);
                    String lastTimestampTimeAgo = topologyStatus.lastTimestampEpoch == -1 ? "-"
                        : timeAgo(System.currentTimeMillis() - topologyStatus.lastTimestampEpoch);
                    PartitionCoordBean partitionCoordBean = new PartitionCoordBean(partition.coord,
                        partition.info.storage,
                        lastIngress,
                        lastQuery,
                        lastTimestamp,
                        lastTimestampTimeAgo);
                    if (state == MiruPartitionState.online) {
                        partitionBean.getOnline().add(partitionCoordBean);
                    } else if (state == MiruPartitionState.upgrading) {
                        partitionBean.getUpgrading().add(partitionCoordBean);
                    } else if (state == MiruPartitionState.obsolete) {
                        partitionBean.getObsolete().add(partitionCoordBean);
                    } else if (state == MiruPartitionState.rebuilding) {
                        partitionBean.getRebuilding().add(partitionCoordBean);
                    } else if (state == MiruPartitionState.bootstrap) {
                        partitionBean.getBootstrap().add(partitionCoordBean);
                    } else if (state == MiruPartitionState.offline) {
                        partitionBean.getOffline().add(partitionCoordBean);
                    }
                    if (topologyStatus.destroyAfterTimestamp > 0 && System.currentTimeMillis() > topologyStatus.destroyAfterTimestamp) {
                        partitionBean.setDestroyed(true);
                    }
                    return partitionBean;
                });
            }

            MiruPartitionId latestPartitionId = miruWALClient.getLargestPartitionId(tenant);
            if (latestPartitionId != null) {
                for (MiruPartitionId latest = latestPartitionId; latest != null; latest = latest.prev()) {
                    partitionsMap.compute(latest, (partitionId, partitionBean) -> {
                        if (partitionBean == null) {
                            partitionBean = new PartitionBean(partitionId.getId());
                        }
                        if (!partitionBean.isDestroyed()) {
                            try {
                                MiruActivityWALStatus status = miruWALClient.getActivityWALStatusForTenant(tenant, partitionId);
                                if (status != null) {
                                    long count = 0;
                                    for (MiruActivityWALStatus.WriterCount writerCount : status.counts) {
                                        count += writerCount.count;
                                    }
                                    partitionBean.setActivityCount(String.valueOf(count));
                                    partitionBean.setBegins(status.begins.size());
                                    partitionBean.setEnds(status.ends.size());
                                }
                            } catch (Exception e) {
                                partitionBean.setErrorMessage(e.getMessage());
                            }
                        }
                        return partitionBean;
                    });
                }
            }

            List<PartitionRange> partitionRanges = clusterRegistry.getIngressRanges(tenant);
            for (PartitionRange partitionRange : partitionRanges) {
                MiruPartitionId partitionId = partitionRange.partitionId;
                partitionsMap.compute(partitionId, (key, partitionBean) -> {
                    if (partitionBean == null) {
                        partitionBean = new PartitionBean(partitionId.getId());
                    }
                    if (!partitionBean.isDestroyed()) {
                        RangeMinMax lookupRange = partitionRange.rangeMinMax;
                        partitionBean.setMinClock(String.valueOf(lookupRange.clockMin));
                        partitionBean.setMaxClock(String.valueOf(lookupRange.clockMax));
                        partitionBean.setMinOrderId(String.valueOf(lookupRange.orderIdMin));
                        partitionBean.setMaxOrderId(String.valueOf(lookupRange.orderIdMax));
                    }
                    return partitionBean;
                });
            }
        } catch (Exception e) {
            log.error("Unable to get partitions for tenant: {}", new Object[] { tenant }, e);
        }

        data.put("tenant", tenant.toString());
        data.put("partitions", partitionsMap.values());

        return renderer.render(template, data);
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
