package com.jivesoftware.os.miru.reader.deployable;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
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
public class MiruPartitionsRegion implements MiruPageRegion<Void> {

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
    public String render(Void input) {
        Map<String, Object> data = Maps.newHashMap();

        try {
            List<Map<String, Object>> partitions = Lists.newArrayList();
            service.expectedTopologies((tenantId, partitionId, host) -> {
                try {
                    service.introspect(tenantId, partitionId, requestContext -> {
                        partitions.add(ImmutableMap.<String, Object>builder()
                            .put("tenantId", tenantId.toString())
                            .put("partitionId", partitionId.toString())
                            .put("activityLastId", requestContext.getActivityIndex().lastId())
                            .put("timeLastId", requestContext.getTimeIndex().lastId())
                            .put("smallestTimestamp", String.valueOf(requestContext.getTimeIndex().getSmallestTimestamp()))
                            .put("largestTimestamp", String.valueOf(requestContext.getTimeIndex().getLargestTimestamp()))
                            .build());
                    });
                    return true;
                } catch (MiruPartitionUnavailableException e) {
                    LOG.debug("Tenant {} partition {} is unavailable", tenantId, partitionId);
                    return true;
                } catch (Exception e) {
                    LOG.error("Failed introspection for tenant {} partition {}", new Object[] { tenantId, partitionId }, e);
                    return false;
                }
            });

            data.put("partitions", partitions);
        } catch (Exception e) {
            LOG.error("Failed to render partitions region", e);
        }

        return renderer.render(template, data);
    }

    @Override
    public String getTitle() {
        return "Partitions";
    }
}
