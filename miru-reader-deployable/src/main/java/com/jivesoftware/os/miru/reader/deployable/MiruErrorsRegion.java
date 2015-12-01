package com.jivesoftware.os.miru.reader.deployable;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.jivesoftware.os.miru.api.MiruPartitionCoord;
import com.jivesoftware.os.miru.service.MiruService;
import com.jivesoftware.os.miru.service.partition.PartitionErrorTracker;
import com.jivesoftware.os.miru.ui.MiruPageRegion;
import com.jivesoftware.os.miru.ui.MiruSoyRenderer;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import java.util.List;
import java.util.Map;

/**
 *
 */
public class MiruErrorsRegion implements MiruPageRegion<Void> {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    private final String template;
    private final MiruSoyRenderer renderer;
    private final MiruService service;
    private final PartitionErrorTracker partitionErrorTracker;

    public MiruErrorsRegion(String template, MiruSoyRenderer renderer, MiruService service, PartitionErrorTracker partitionErrorTracker) {
        this.template = template;
        this.renderer = renderer;
        this.service = service;
        this.partitionErrorTracker = partitionErrorTracker;
    }

    @Override
    public String render(Void input) {
        Map<String, Object> data = Maps.newHashMap();

        try {
            List<Map<String, Object>> sinceRebuild = Lists.newArrayList();
            for (Map.Entry<MiruPartitionCoord, List<String>> entry : partitionErrorTracker.getErrorsSinceRebuild().entrySet()) {
                MiruPartitionCoord coord = entry.getKey();
                service.introspect(coord.tenantId, coord.partitionId, (requestContext, state, storage) -> {
                    sinceRebuild.add(ImmutableMap.of("tenantId", coord.tenantId.toString(),
                        "partitionId", coord.partitionId.toString(),
                        "state", state != null ? state.name() : "unknown",
                        "storage", storage != null ? storage.name() : "unknown",
                        "reasons", entry.getValue()));
                });
            }

            data.put("sinceRebuild", sinceRebuild);

            List<Map<String, Object>> sinceStartup = Lists.newArrayList();
            for (MiruPartitionCoord coord : partitionErrorTracker.getErrorsSinceStartup()) {
                sinceStartup.add(ImmutableMap.of("tenantId", coord.tenantId.toString(),
                    "partitionId", coord.partitionId.toString()));
            }

            data.put("sinceStartup", sinceStartup);

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
