package com.jivesoftware.os.miru.manage.deployable.region;

import com.google.common.base.Charsets;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.jivesoftware.os.jive.utils.logger.MetricLogger;
import com.jivesoftware.os.jive.utils.logger.MetricLoggerFactory;
import com.jivesoftware.os.miru.api.MiruBackingStorage;
import com.jivesoftware.os.miru.api.MiruHost;
import com.jivesoftware.os.miru.api.MiruPartition;
import com.jivesoftware.os.miru.api.MiruPartitionCoordMetrics;
import com.jivesoftware.os.miru.api.MiruPartitionState;
import com.jivesoftware.os.miru.api.MiruTopologyStatus;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.cluster.MiruClusterRegistry;
import com.jivesoftware.os.miru.manage.deployable.MiruSoyRenderer;
import com.jivesoftware.os.miru.manage.deployable.region.bean.PartitionCoordBean;
import com.jivesoftware.os.miru.manage.deployable.region.bean.TenantBean;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;

/**
 *
 */
public class MiruHostFocusRegion implements MiruRegion<MiruHost> {

    private static final MetricLogger log = MetricLoggerFactory.getLogger();

    private final String template;
    private final MiruSoyRenderer renderer;
    private final MiruClusterRegistry clusterRegistry;

    public MiruHostFocusRegion(String template, MiruSoyRenderer renderer, MiruClusterRegistry clusterRegistry) {
        this.template = template;
        this.renderer = renderer;
        this.clusterRegistry = clusterRegistry;
    }

    @Override
    public String render(MiruHost host) {
        Map<String, Object> data = Maps.newHashMap();
        List<MiruTenantId> tenantsForHost = Lists.newArrayList();
        try {
            tenantsForHost = clusterRegistry.getTenantsForHost(host);
        } catch (Exception e) {
            log.error("Unable to retrieve tenants for host: " + host, e);
        }

        SortedMap<MiruTenantId, TenantBean> tenantsMap = Maps.newTreeMap();
        for (MiruTenantId tenantId : tenantsForHost) {
            List<MiruTopologyStatus> statusForTenantHost;
            try {
                statusForTenantHost = clusterRegistry.getTopologyStatusForTenantHost(tenantId, host);
            } catch (Exception e) {
                log.error("Unable to get partitions for tenant: " + new String(tenantId.getBytes(), Charsets.UTF_8) + " and host: " + host, e);
                statusForTenantHost = Lists.newArrayList();
            }

            for (MiruTopologyStatus status : statusForTenantHost) {
                TenantBean tenantBean = tenantsMap.get(tenantId);
                if (tenantBean == null) {
                    tenantBean = new TenantBean(tenantId);
                    tenantsMap.put(tenantId, tenantBean);
                }
                MiruPartition partition = status.partition;
                MiruPartitionCoordMetrics metrics = status.metrics;
                MiruPartitionState state = (partition != null) ? partition.info.state : MiruPartitionState.offline;
                MiruBackingStorage storage = (partition != null) ? partition.info.storage : MiruBackingStorage.unknown;
                PartitionCoordBean partitionCoordBean = new PartitionCoordBean(
                        partition != null ? partition.coord : null,
                        storage,
                        metrics.sizeInMemory,
                        metrics.sizeOnDisk);
                if (state == MiruPartitionState.online) {
                    tenantBean.getOnline().add(partitionCoordBean);
                } else if (state == MiruPartitionState.rebuilding) {
                    tenantBean.getRebuilding().add(partitionCoordBean);
                } else if (state == MiruPartitionState.bootstrap) {
                    tenantBean.getBootstrap().add(partitionCoordBean);
                } else if (state == MiruPartitionState.offline) {
                    tenantBean.getOffline().add(partitionCoordBean);
                }
            }
        }

        data.put("host", host);
        data.put("tenants", tenantsMap.values());

        return renderer.render(template, data);
    }

}
