package com.jivesoftware.os.miru.writer.deployable.region;

import com.google.common.base.Charsets;
import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Maps;
import com.google.common.collect.Table;
import com.jivesoftware.os.miru.api.MiruPartition;
import com.jivesoftware.os.miru.api.activity.MiruPartitionId;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.wal.activity.MiruActivityWALReader;
import com.jivesoftware.os.miru.writer.deployable.MiruSoyRenderer;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import java.util.Map;

/**
 *
 */
public class MiruRepairRegion implements MiruPageRegion<Optional<MiruTenantId>> {

    private static final MetricLogger log = MetricLoggerFactory.getLogger();

    private final int maxAllowedGap = 10;

    private final String template;
    private final MiruSoyRenderer renderer;
    private final MiruActivityWALReader activityWALReader;

    public MiruRepairRegion(String template,
        MiruSoyRenderer renderer,
        MiruActivityWALReader activityWALReader) {
        this.template = template;
        this.renderer = renderer;
        this.activityWALReader = activityWALReader;
    }

    @Override
    public String render(Optional<MiruTenantId> optionalTenantId) {
        Map<String, Object> data = Maps.newHashMap();

        if (optionalTenantId.isPresent()) {
            MiruTenantId tenantId = optionalTenantId.get();
            data.put("tenant", new String(tenantId.getBytes(), Charsets.UTF_8));
        }

        try {
            final Table<String, String, String> badPartitions = HashBasedTable.create();
            activityWALReader.allPartitions(new MiruActivityWALReader.PartitionsStream() {
                MiruTenantId currentTenantId = null;
                MiruPartitionId lastPartitionId = null;

                @Override
                public boolean stream(MiruTenantId tenantId, MiruPartitionId partitionId) throws Exception {
                    if (tenantId != null && partitionId != null) {
                        if (currentTenantId == null || !currentTenantId.equals(tenantId)) {
                            currentTenantId = tenantId;
                            lastPartitionId = null;
                        }

                        if (lastPartitionId != null && (partitionId.getId() - lastPartitionId.getId()) > maxAllowedGap) {
                            badPartitions.put(tenantId.toString(), partitionId.toString(),
                                "Followed gap of " + (partitionId.getId() - lastPartitionId.getId()));
                        } else if (partitionId.getId() < 0) {
                            badPartitions.put(tenantId.toString(), partitionId.toString(), "Negative");
                        }
                        lastPartitionId = partitionId;
                    }
                    return true;
                }
            });

            data.put("badPartitions", badPartitions.rowMap());
        } catch (Exception e) {
            log.error("Failed to find bad partitions", e);
        }

        return renderer.render(template, data);
    }

    private final Function<MiruPartition, MiruPartitionId> partitionToId = new Function<MiruPartition, MiruPartitionId>() {
        @Override
        public MiruPartitionId apply(MiruPartition input) {
            return input != null ? input.coord.partitionId : null;
        }
    };

    @Override
    public String getTitle() {
        return "Activity WAL";
    }
}
