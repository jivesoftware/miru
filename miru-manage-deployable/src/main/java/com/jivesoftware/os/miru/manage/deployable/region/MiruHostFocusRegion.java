package com.jivesoftware.os.miru.manage.deployable.region;

import com.google.common.base.Charsets;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.jivesoftware.os.jive.utils.ordered.id.JiveEpochTimestampProvider;
import com.jivesoftware.os.jive.utils.ordered.id.SnowflakeIdPacker;
import com.jivesoftware.os.miru.api.MiruBackingStorage;
import com.jivesoftware.os.miru.api.MiruHost;
import com.jivesoftware.os.miru.api.MiruPartition;
import com.jivesoftware.os.miru.api.MiruPartitionState;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.api.topology.MiruTopologyStatus;
import com.jivesoftware.os.miru.cluster.MiruClusterRegistry;
import com.jivesoftware.os.miru.manage.deployable.region.bean.PartitionCoordBean;
import com.jivesoftware.os.miru.manage.deployable.region.bean.TenantBean;
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
public class MiruHostFocusRegion implements MiruRegion<MiruHost> {

    private static final MetricLogger log = MetricLoggerFactory.getLogger();

    private final String template;
    private final MiruSoyRenderer renderer;
    private final MiruClusterRegistry clusterRegistry;

    private final SnowflakeIdPacker idPacker = new SnowflakeIdPacker();

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
                MiruPartitionState state = (partition != null) ? partition.info.state : MiruPartitionState.offline;
                MiruBackingStorage storage = (partition != null) ? partition.info.storage : MiruBackingStorage.unknown;
                String lastIngress = timeAgo(System.currentTimeMillis() - status.lastIngressTimestamp);
                String lastQuery = timeAgo(System.currentTimeMillis() - status.lastQueryTimestamp);
                String lastTimestamp = String.valueOf(status.lastTimestamp);
                long lastTimestampEpoch = status.lastTimestamp == -1 ? -1
                    : idPacker.unpack(status.lastTimestampEpoch)[0] + JiveEpochTimestampProvider.JIVE_EPOCH;
                String lastTimestampTimeAgo = lastTimestampEpoch == -1 ? "-" : timeAgo(System.currentTimeMillis() - lastTimestampEpoch);
                PartitionCoordBean partitionCoordBean = new PartitionCoordBean(
                    partition != null ? partition.coord : null,
                    storage, lastIngress, lastQuery, lastTimestamp, lastTimestampTimeAgo);
                if (state == MiruPartitionState.online) {
                    tenantBean.getOnline().add(partitionCoordBean);
                } else if (state == MiruPartitionState.upgrading) {
                    tenantBean.getUpgrading().add(partitionCoordBean);
                } else if (state == MiruPartitionState.obsolete) {
                    tenantBean.getObsolete().add(partitionCoordBean);
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
