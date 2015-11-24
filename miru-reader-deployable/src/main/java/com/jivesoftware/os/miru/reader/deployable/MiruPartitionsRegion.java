package com.jivesoftware.os.miru.reader.deployable;

import com.google.common.base.Charsets;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.jivesoftware.os.filer.io.api.StackBuffer;
import com.jivesoftware.os.miru.api.activity.MiruPartitionId;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.api.wal.MiruSipCursor;
import com.jivesoftware.os.miru.plugin.partition.MiruPartitionUnavailableException;
import com.jivesoftware.os.miru.service.MiruService;
import com.jivesoftware.os.miru.ui.MiruPageRegion;
import com.jivesoftware.os.miru.ui.MiruSoyRenderer;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import java.util.List;
import java.util.Map;

/**
 *
 */
public class MiruPartitionsRegion implements MiruPageRegion<Optional<String>> {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    private final String template;
    private final MiruSoyRenderer renderer;
    private final MiruService service;

    public MiruPartitionsRegion(String template, MiruSoyRenderer renderer, MiruService service) {
        this.template = template;
        this.renderer = renderer;
        this.service = service;
    }

    @Override
    public String render(Optional<String> input) {
        Map<String, Object> data = Maps.newHashMap();

        try {
            String tenant = input.or("").trim();
            data.put("tenant", tenant);

            if (!tenant.isEmpty()) {
                List<Map<String, Object>> partitions = Lists.newArrayList();
                if (tenant.equals("*")) {
                    service.expectedTopologies(Optional.<MiruTenantId>absent(),
                        (tenantId, partitionId, host) -> inspect(tenantId, partitionId, partitions));
                } else {
                    service.expectedTopologies(Optional.of(new MiruTenantId(tenant.getBytes(Charsets.UTF_8))),
                        (tenantId, partitionId, host) -> inspect(tenantId, partitionId, partitions));
                }
                data.put("partitions", partitions);
            }
        } catch (Exception e) {
            LOG.error("Failed to render partitions region", e);
        }

        return renderer.render(template, data);
    }

    private boolean inspect(MiruTenantId tenantId, MiruPartitionId partitionId, List<Map<String, Object>> partitions) {
        try {
            StackBuffer stackBuffer = new StackBuffer();
            service.introspect(tenantId, partitionId, requestContext -> {
                Optional<? extends MiruSipCursor<?>> sip = requestContext.getSipIndex().getSip(stackBuffer);
                partitions.add(ImmutableMap.<String, Object>builder()
                    .put("tenantId", tenantId.toString())
                    .put("partitionId", partitionId.toString())
                    .put("activityLastId", requestContext.getActivityIndex().lastId(stackBuffer))
                    .put("timeLastId", requestContext.getTimeIndex().lastId())
                    .put("smallestTimestamp", String.valueOf(requestContext.getTimeIndex().getSmallestTimestamp()))
                    .put("largestTimestamp", String.valueOf(requestContext.getTimeIndex().getLargestTimestamp()))
                    .put("sip", sip.isPresent() ? sip.get().toString() : "")
                    .build());
            });
            return true;
        } catch (MiruPartitionUnavailableException e) {
            LOG.debug("Tenant {} partition {} is unavailable", tenantId, partitionId);
            return true;
        } catch (Exception e) {
            LOG.error("Failed introspection for tenant {} partition {}", new Object[]{tenantId, partitionId}, e);
            return false;
        }
    }

    @Override
    public String getTitle() {
        return "Partitions";
    }
}
