package com.jivesoftware.os.miru.writer.deployable.region;

import com.google.common.base.Charsets;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.jivesoftware.os.miru.api.activity.MiruPartitionId;
import com.jivesoftware.os.miru.api.activity.MiruPartitionedActivity;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.api.wal.MiruActivityWALStatus;
import com.jivesoftware.os.miru.api.wal.MiruWALClient;
import com.jivesoftware.os.miru.api.wal.MiruWALEntry;
import com.jivesoftware.os.miru.wal.MiruWALDirector;
import com.jivesoftware.os.miru.writer.deployable.MiruSoyRenderer;
import com.jivesoftware.os.miru.writer.deployable.region.bean.WALBean;
import com.jivesoftware.os.miru.writer.deployable.region.input.MiruActivityWALRegionInput;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 *
 */
public class MiruActivityWALRegion implements MiruPageRegion<MiruActivityWALRegionInput> {

    private static final MetricLogger log = MetricLoggerFactory.getLogger();

    private final String template;
    private final MiruSoyRenderer renderer;
    private final MiruWALDirector miruWALDirector;

    public MiruActivityWALRegion(String template,
        MiruSoyRenderer renderer,
        MiruWALDirector miruWALDirector) {
        this.template = template;
        this.renderer = renderer;
        this.miruWALDirector = miruWALDirector;
    }

    @Override
    public String render(MiruActivityWALRegionInput miruActivityWALRegionInput) {
        Map<String, Object> data = Maps.newHashMap();

        Optional<MiruTenantId> optionalTenantId = miruActivityWALRegionInput.getTenantId();
        Optional<MiruPartitionId> optionalPartitionId = miruActivityWALRegionInput.getPartitionId();

        if (optionalTenantId.isPresent()) {
            MiruTenantId tenantId = optionalTenantId.get();
            data.put("tenant", new String(tenantId.getBytes(), Charsets.UTF_8));

            try {
                MiruPartitionId latestPartitionId = miruWALDirector.getLargestPartitionIdAcrossAllWriters(tenantId);
                List<MiruPartitionId> partitionIds = Lists.newArrayList();
                if (latestPartitionId != null) {
                    for (MiruPartitionId latest = latestPartitionId; latest != null; latest = latest.prev()) {
                        partitionIds.add(latest);
                    }
                    Collections.reverse(partitionIds);
                }

                List<MiruActivityWALStatus> partitionStatuses = miruWALDirector.getPartitionStatus(tenantId, partitionIds);
                List<Map<String, String>> partitions = Lists.newArrayList();
                for (MiruActivityWALStatus status : partitionStatuses) {
                    partitions.add(ImmutableMap.<String, String>of(
                        "id", status.partitionId.toString(),
                        "count", String.valueOf(status.count),
                        "begins", String.valueOf(status.begins.size()),
                        "ends", String.valueOf(status.ends.size())));
                }
                data.put("partitions", partitions);

                if (optionalPartitionId.isPresent()) {
                    MiruPartitionId partitionId = optionalPartitionId.get();
                    data.put("partition", partitionId.getId());

                    List<WALBean> walActivities = Lists.newArrayList();
                    final boolean sip = miruActivityWALRegionInput.getSip().or(false);
                    final int limit = miruActivityWALRegionInput.getLimit().or(100);
                    long afterTimestamp = miruActivityWALRegionInput.getAfterTimestamp().or(0l);
                    final AtomicLong lastTimestamp = new AtomicLong();
                    try {
                        if (sip) {
                            final MiruWALClient.StreamBatch<MiruWALEntry, MiruWALClient.SipActivityCursor> sipped = miruWALDirector.sipActivity(tenantId,
                                partitionId,
                                new MiruWALClient.SipActivityCursor(MiruPartitionedActivity.Type.ACTIVITY.getSort(),
                                    afterTimestamp,
                                    0),
                                limit);

                            walActivities = Lists.transform(sipped.batch,
                                input -> new WALBean(input.collisionId, Optional.of(input.activity), input.version));
                            lastTimestamp.set(sipped.cursor != null ? sipped.cursor.collisionId : Long.MAX_VALUE);

                        } else {
                            MiruWALClient.StreamBatch<MiruWALEntry, MiruWALClient.GetActivityCursor> gopped = miruWALDirector.getActivity(tenantId,
                                partitionId,
                                new MiruWALClient.GetActivityCursor(MiruPartitionedActivity.Type.ACTIVITY.getSort(), afterTimestamp),
                                limit);
                            walActivities = Lists.transform(gopped.batch,
                                input -> new WALBean(input.collisionId, Optional.of(input.activity), input.version));
                            lastTimestamp.set(gopped.cursor != null ? gopped.cursor.collisionId : Long.MAX_VALUE);

                        }
                    } catch (Exception e) {
                        log.error("Failed to read activity WAL", e);
                        data.put("error", e.getMessage());
                    }
                    data.put("sip", sip);
                    data.put("limit", limit);
                    data.put("afterTimestamp", String.valueOf(afterTimestamp));
                    data.put("activities", walActivities);
                    data.put("nextTimestamp", String.valueOf(lastTimestamp.get() + 1));
                }
            } catch (Exception e) {
                log.error("Failed to get partitions for tenant", e);
                data.put("error", e.getMessage());
            }
        }

        return renderer.render(template, data);
    }

    @Override
    public String getTitle() {
        return "Activity WAL";
    }
}
