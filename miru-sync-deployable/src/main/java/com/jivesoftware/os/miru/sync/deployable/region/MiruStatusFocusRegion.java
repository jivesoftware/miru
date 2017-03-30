package com.jivesoftware.os.miru.sync.deployable.region;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.jivesoftware.os.miru.api.activity.MiruPartitionId;
import com.jivesoftware.os.miru.api.wal.MiruCursor;
import com.jivesoftware.os.miru.sync.deployable.MiruSyncSender;
import com.jivesoftware.os.miru.sync.deployable.MiruSyncSenders;
import com.jivesoftware.os.miru.ui.MiruRegion;
import com.jivesoftware.os.miru.ui.MiruSoyRenderer;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Map;

/**
 *
 */
public class MiruStatusFocusRegion implements MiruRegion<MiruStatusRegionInput> {

    private static final MetricLogger log = MetricLoggerFactory.getLogger();

    private final String template;
    private final MiruSoyRenderer renderer;
    private final MiruSyncSenders<?, ?> syncSenders;
    private final ObjectMapper mapper;

    public MiruStatusFocusRegion(String template,
        MiruSoyRenderer renderer,
        MiruSyncSenders<?, ?> syncSenders,
        ObjectMapper mapper) {

        this.template = template;
        this.renderer = renderer;
        this.syncSenders = syncSenders;
        this.mapper = mapper;
    }

    @Override
    public String render(MiruStatusRegionInput input) {
        Map<String, Object> data = Maps.newHashMap();

        SimpleDateFormat dateFormat = new SimpleDateFormat("EEE, dd MMM yyyy H:mm:ss z");

        data.put("syncspaceName", input.syncspaceName);
        data.put("tenant", input.tenantId.toString());
        try {
            List<Map<String, Object>> progress = Lists.newArrayList();
            if (syncSenders != null) {
                MiruSyncSender<?, ?> syncSender = syncSenders.getSender(input.syncspaceName);
                if (syncSender != null) {
                    syncSender.streamProgress(input.tenantId, null, (fromTenantId, toTenantId, type, partitionId, timestamp, taking) -> {
                        MiruCursor<?, ?> cursor = syncSender.getTenantPartitionCursor(input.tenantId, toTenantId, MiruPartitionId.of(partitionId));
                        progress.add(ImmutableMap.<String, Object>builder()
                            .put("name", syncSender.getConfig().name)
                            .put("from", fromTenantId.toString())
                            .put("to", toTenantId.toString())
                            .put("type", type.name())
                            .put("partitionId", String.valueOf(partitionId))
                            .put("time", dateFormat.format(new Date(timestamp)))
                            .put("taking", taking)
                            .put("cursor", cursor == null ? "-" : mapper.writeValueAsString(cursor))
                            .build());
                        return true;
                    });
                }
            } else {
                data.put("warning", "Sync sender is not enabled");
            }
            data.put("progress", progress);
        } catch (Exception e) {
            log.error("Unable to get progress for syncspace:{} tenant:{}", new Object[] { input.syncspaceName, input.tenantId }, e);
        }

        return renderer.render(template, data);
    }
}
