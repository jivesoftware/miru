package com.jivesoftware.os.miru.wal.deployable.region;

import com.google.common.base.Charsets;
import com.google.common.base.Optional;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Table;
import com.google.common.collect.TreeBasedTable;
import com.jivesoftware.os.miru.api.activity.MiruPartitionId;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.api.wal.MiruWALClient;
import com.jivesoftware.os.miru.ui.MiruPageRegion;
import com.jivesoftware.os.miru.ui.MiruSoyRenderer;
import com.jivesoftware.os.miru.wal.activity.MiruActivityWALReader;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 *
 */
public class MiruRepairRegion implements MiruPageRegion<Optional<String>> {

    private static final MetricLogger log = MetricLoggerFactory.getLogger();

    private final int maxAllowedGap = 10;

    private final String template;
    private final MiruSoyRenderer renderer;
    private final MiruActivityWALReader activityWALReader;
    private final MiruWALClient<?, ?> miruWALClient;

    public MiruRepairRegion(String template,
        MiruSoyRenderer renderer,
        MiruActivityWALReader activityWALReader,
        MiruWALClient<?, ?> miruWALClient) {
        this.template = template;
        this.renderer = renderer;
        this.activityWALReader = activityWALReader;
        this.miruWALClient = miruWALClient;
    }

    @Override
    public String render(Optional<String> optionalTenantId) {
        Map<String, Object> data = Maps.newHashMap();

        try {
            if (optionalTenantId.isPresent()) {
                String tenantId = optionalTenantId.get();
                data.put("tenant", tenantId);
                if (tenantId.equals("*")) {
                    data.put("partitions", getBadPartitions().rowMap());
                } else {
                    data.put("partitions", getTenantPartitions(new MiruTenantId(tenantId.getBytes(Charsets.UTF_8))).rowMap());
                }
            }
        } catch (Exception e) {
            log.error("Failed to find bad partitions", e);
        }

        return renderer.render(template, data);
    }

    private Table<String, String, String> getTenantPartitions(MiruTenantId tenantId) throws Exception {
        final Table<String, String, String> tenantPartitions = TreeBasedTable.create(); // tree for order
        MiruPartitionId latestPartitionId = miruWALClient.getLargestPartitionId(tenantId);
        if (latestPartitionId != null) {
            for (MiruPartitionId latest = latestPartitionId; latest != null; latest = latest.prev()) {
                tenantPartitions.put(tenantId.toString(), latest.toString(), "");
            }
        }
        return tenantPartitions;
    }

    private Table<String, String, String> getBadPartitions() throws Exception {
        final ListMultimap<MiruTenantId, MiruPartitionId> allPartitions = ArrayListMultimap.create();
        final AtomicLong count = new AtomicLong(0);
        activityWALReader.allPartitions((tenantId, partitionId) -> {
            if (tenantId != null && partitionId != null) {
                allPartitions.put(tenantId, partitionId);
                long got = count.incrementAndGet();
                if (got % 1_000 == 0) {
                    log.info("Repair has scanned {} partitions", got);
                }
            }
            return true;
        });

        final Table<String, String, String> badPartitions = TreeBasedTable.create(); // tree for order
        for (Map.Entry<MiruTenantId, Collection<MiruPartitionId>> entry : allPartitions.asMap().entrySet()) {
            MiruTenantId tenantId = entry.getKey();
            List<MiruPartitionId> partitionIds = Lists.newArrayList(entry.getValue());
            Collections.sort(partitionIds);

            MiruPartitionId lastPartitionId = null;
            for (MiruPartitionId partitionId : partitionIds) {
                if (lastPartitionId != null && (partitionId.getId() - lastPartitionId.getId()) > maxAllowedGap) {
                    badPartitions.put(tenantId.toString(), partitionId.toString(),
                        "Followed gap of " + (partitionId.getId() - lastPartitionId.getId()));
                } else if (partitionId.getId() < 0) {
                    badPartitions.put(tenantId.toString(), partitionId.toString(), "Negative");
                }
                lastPartitionId = partitionId;
            }
        }
        return badPartitions;
    }

    @Override
    public String getTitle() {
        return "Repair";
    }
}
