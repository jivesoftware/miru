package com.jivesoftware.os.miru.manage.deployable.region;

import com.google.common.collect.Maps;
import com.jivesoftware.os.miru.api.MiruPartition;
import com.jivesoftware.os.miru.api.MiruPartitionState;
import com.jivesoftware.os.miru.api.MiruTopologyStatus;
import com.jivesoftware.os.miru.api.activity.MiruPartitionId;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.api.topology.MiruTenantConfig;
import com.jivesoftware.os.miru.cluster.MiruClusterRegistry;
import com.jivesoftware.os.miru.cluster.MiruTenantConfigFields;
import com.jivesoftware.os.miru.manage.deployable.MiruSoyRenderer;
import com.jivesoftware.os.miru.manage.deployable.region.bean.PartitionBean;
import com.jivesoftware.os.miru.manage.deployable.region.bean.PartitionCoordBean;
import com.jivesoftware.os.miru.wal.activity.MiruActivityWALReader;
import com.jivesoftware.os.miru.wal.activity.MiruActivityWALStatus;
import com.jivesoftware.os.miru.wal.partition.MiruPartitionIdProvider;
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
    private final MiruActivityWALReader activityWALReader;
    private final MiruPartitionIdProvider partitionIdProvider;

    public MiruTenantEntryRegion(String template,
        MiruSoyRenderer renderer,
        MiruClusterRegistry clusterRegistry,
        MiruActivityWALReader activityWALReader,
        MiruPartitionIdProvider partitionIdProvider) {

        this.template = template;
        this.renderer = renderer;
        this.clusterRegistry = clusterRegistry;
        this.activityWALReader = activityWALReader;
        this.partitionIdProvider = partitionIdProvider;
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

            MiruPartitionId latestPartitionId = partitionIdProvider.getLargestPartitionIdAcrossAllWriters(tenant);

            if (latestPartitionId != null) {
                for (MiruPartitionId latest = latestPartitionId; latest != null; latest = latest.prev()) {
                    MiruActivityWALStatus status = activityWALReader.getStatus(tenant, latest);
                    partitionsMap.put(latest, new PartitionBean(latest.getId(), status.count, status.begins.size(), status.ends.size()));
                }
            }

            for (MiruTopologyStatus topologyStatus : statusForTenant) {
                MiruPartition partition = topologyStatus.partition;
                MiruPartitionId partitionId = partition.coord.partitionId;
                PartitionBean partitionBean = partitionsMap.get(partitionId);
                if (partitionBean == null) {
                    MiruActivityWALStatus status = activityWALReader.getStatus(tenant, partitionId);
                    partitionBean = new PartitionBean(partitionId.getId(), status.count, status.begins.size(), status.ends.size());
                    partitionsMap.put(partitionId, partitionBean);
                }
                MiruPartitionState state = partition.info.state;
                String idle = timeAgo(System.currentTimeMillis() - topologyStatus.lastActiveTimestamp);
                PartitionCoordBean partitionCoordBean = new PartitionCoordBean(partition.coord, partition.info.storage, idle);
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
        } catch (Exception e) {
            log.error("Unable to get partitions for tenant: " + tenant);
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
