package com.jivesoftware.os.miru.sync.deployable.region;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.jivesoftware.os.miru.api.activity.MiruPartitionId;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.api.wal.MiruCursor;
import com.jivesoftware.os.miru.sync.deployable.MiruSyncSender;
import com.jivesoftware.os.miru.ui.MiruRegion;
import com.jivesoftware.os.miru.ui.MiruSoyRenderer;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import java.util.List;
import java.util.Map;

/**
 *
 */
public class MiruStatusFocusRegion implements MiruRegion<MiruTenantId> {

    private static final MetricLogger log = MetricLoggerFactory.getLogger();

    private final String template;
    private final MiruSoyRenderer renderer;
    private final MiruSyncSender<?, ?> syncSender;
    private final ObjectMapper mapper;

    public MiruStatusFocusRegion(String template,
        MiruSoyRenderer renderer,
        MiruSyncSender<?, ?> syncSender,
        ObjectMapper mapper) {

        this.template = template;
        this.renderer = renderer;
        this.syncSender = syncSender;
        this.mapper = mapper;
    }

    @Override
    public String render(MiruTenantId tenantId) {
        Map<String, Object> data = Maps.newHashMap();

        data.put("tenant", tenantId.toString());
        try {
            List<Map<String, Object>> progress = Lists.newArrayList();
            if (syncSender != null) {
                syncSender.streamProgress(tenantId, null, (toTenantId, type, partitionId) -> {
                    MiruCursor<?, ?> cursor = syncSender.getTenantPartitionCursor(tenantId, toTenantId, MiruPartitionId.of(partitionId));
                    progress.add(ImmutableMap.of(
                        "toTenantId", toTenantId.toString(),
                        "type", type.name(),
                        "partitionId", String.valueOf(partitionId),
                        "cursor", mapper.writeValueAsString(cursor)));
                    return true;
                });
            } else {
                data.put("warning", "Sync sender is not enabled");
            }
            data.put("progress", progress);
        } catch (Exception e) {
            log.error("Unable to get progress for tenant: {}", new Object[] { tenantId }, e);
        }

        return renderer.render(template, data);
    }
}
