package com.jivesoftware.os.miru.manage.deployable.region;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.jivesoftware.os.jive.utils.logger.MetricLogger;
import com.jivesoftware.os.jive.utils.logger.MetricLoggerFactory;
import com.jivesoftware.os.miru.api.MiruPartition;
import com.jivesoftware.os.miru.api.MiruPartitionCoordMetrics;
import com.jivesoftware.os.miru.api.MiruPartitionState;
import com.jivesoftware.os.miru.api.MiruTopologyStatus;
import com.jivesoftware.os.miru.api.activity.MiruPartitionId;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.cluster.MiruClusterRegistry;
import com.jivesoftware.os.miru.cluster.MiruTenantConfig;
import com.jivesoftware.os.miru.cluster.MiruTenantConfigFields;
import com.jivesoftware.os.miru.manage.deployable.MiruSoyRenderer;
import com.jivesoftware.os.miru.manage.deployable.region.bean.PartitionBean;
import com.jivesoftware.os.miru.manage.deployable.region.bean.PartitionCoordBean;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;

/**
 *
 */
public class MiruTenantEntryRegion implements MiruRegion<MiruTenantId> {

    private static final MetricLogger log = MetricLoggerFactory.getLogger();

    private final String template;
    private final MiruSoyRenderer renderer;
    private final MiruClusterRegistry clusterRegistry;

    public MiruTenantEntryRegion(String template, MiruSoyRenderer renderer, MiruClusterRegistry clusterRegistry) {
        this.template = template;
        this.renderer = renderer;
        this.clusterRegistry = clusterRegistry;
    }

    @Override
    public String render(MiruTenantId tenant) {
        Map<String, Object> data = Maps.newHashMap();
        try {
            MiruTenantConfig config = clusterRegistry.getTenantConfig(tenant);
            data.put("config", true);
            data.put("configNumberOfReplicas", String.valueOf(config.getLong(MiruTenantConfigFields.number_of_replicas, -1)));
            data.put("configTopologyIsStaleAfterMillis", String.valueOf(config.getLong(MiruTenantConfigFields.topology_is_stale_after_millis, -1)));
        } catch (Exception e) {
            log.error("Failed to get config for tenant: " + tenant);
            data.put("config", false);
        }

        List<MiruTopologyStatus> statusForTenant;
        try {
            statusForTenant = clusterRegistry.getTopologyStatusForTenant(tenant);
        } catch (Exception e) {
            log.error("Unable to get partitions for tenant: " + tenant);
            statusForTenant = Lists.newArrayList();
        }

        SortedMap<MiruPartitionId, PartitionBean> partitionsMap = Maps.newTreeMap();
        for (MiruTopologyStatus status : statusForTenant) {
            MiruPartition partition = status.partition;
            MiruPartitionId partitionId = partition.coord.partitionId;
            PartitionBean partitionBean = partitionsMap.get(partitionId);
            if (partitionBean == null) {
                partitionBean = new PartitionBean(partitionId.getId());
                partitionsMap.put(partitionId, partitionBean);
            }
            MiruPartitionState state = partition.info.state;
            MiruPartitionCoordMetrics metrics = status.metrics;
            PartitionCoordBean partitionCoordBean = new PartitionCoordBean(partition.coord, partition.info.storage, metrics.sizeInMemory, metrics.sizeOnDisk);
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

        data.put("tenant", tenant.toString());
        data.put("partitionsCount", statusForTenant.size());
        data.put("partitions", partitionsMap.values());

        return renderer.render(template, data);
    }

}
