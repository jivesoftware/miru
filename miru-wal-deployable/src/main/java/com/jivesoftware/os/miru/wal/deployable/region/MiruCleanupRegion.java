package com.jivesoftware.os.miru.wal.deployable.region;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.jivesoftware.os.miru.api.activity.MiruPartitionId;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.api.topology.MiruPartitionStatus;
import com.jivesoftware.os.miru.ui.MiruPageRegion;
import com.jivesoftware.os.miru.ui.MiruSoyRenderer;
import com.jivesoftware.os.miru.wal.MiruWALDirector;
import com.jivesoftware.os.mlogger.core.ISO8601DateFormat;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import java.util.Date;
import java.util.List;
import java.util.Map;

/**
 *
 */
public class MiruCleanupRegion implements MiruPageRegion<Void> {

    private static final MetricLogger log = MetricLoggerFactory.getLogger();

    private final String template;
    private final MiruSoyRenderer renderer;
    private final MiruWALDirector<?, ?> miruWALDirector;

    public MiruCleanupRegion(String template,
        MiruSoyRenderer renderer,
        MiruWALDirector<?, ?> miruWALDirector) {
        this.template = template;
        this.renderer = renderer;
        this.miruWALDirector = miruWALDirector;
    }

    @Override
    public String render(Void input) {
        Map<String, Object> data = Maps.newHashMap();

        ISO8601DateFormat dateFormat = new ISO8601DateFormat();
        try {
            List<Map<String, Object>> tenantData = Lists.newArrayList();
            List<MiruTenantId> tenantIds = miruWALDirector.getAllTenantIds();
            int count = 0;
            for (MiruTenantId tenantId : tenantIds) {
                count++;
                log.info("Gathering partition status for tenant {}, {}/{}", tenantId, count, tenantIds.size());
                List<MiruPartitionStatus> status = miruWALDirector.getAllPartitionStatus(tenantId);
                MiruPartitionId latestPartitionId = null;
                for (MiruPartitionStatus partitionStatus : status) {
                    if (latestPartitionId == null || latestPartitionId.compareTo(partitionStatus.getPartitionId()) < 0) {
                        latestPartitionId = partitionStatus.getPartitionId();
                    }
                }

                List<Map<String, Object>> partitions = Lists.newArrayList();
                for (MiruPartitionStatus partitionStatus : status) {
                    if (partitionStatus.getDestroyAfterTimestamp() > 0 && System.currentTimeMillis() > partitionStatus.getDestroyAfterTimestamp()) {
                        partitions.add(ImmutableMap.of(
                            "partitionId", String.valueOf(partitionStatus.getPartitionId()),
                            "lastIngressTimestamp", dateFormat.format(new Date(partitionStatus.getLastIngressTimestamp())),
                            "destroyAfterTimestamp", dateFormat.format(new Date(partitionStatus.getDestroyAfterTimestamp())),
                            "cleanupAfterTimestamp", dateFormat.format(new Date(partitionStatus.getCleanupAfterTimestamp())),
                            "isLatest", partitionStatus.getPartitionId().equals(latestPartitionId)));
                    }
                }
                if (!partitions.isEmpty()) {
                    tenantData.add(ImmutableMap.of(
                        "tenantId", tenantId.toString(),
                        "partitions", partitions));
                }
            }
            data.put("tenants", tenantData);
        } catch (Exception e) {
            log.error("Failed to get partition status", e);
        }

        return renderer.render(template, data);
    }

    @Override
    public String getTitle() {
        return "Cleanup";
    }
}
