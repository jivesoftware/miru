package com.jivesoftware.os.miru.manage.deployable.region;

import com.google.common.base.Charsets;
import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.jivesoftware.os.jive.utils.logger.MetricLogger;
import com.jivesoftware.os.jive.utils.logger.MetricLoggerFactory;
import com.jivesoftware.os.miru.api.MiruPartition;
import com.jivesoftware.os.miru.api.activity.MiruPartitionId;
import com.jivesoftware.os.miru.api.activity.MiruPartitionedActivity;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.cluster.MiruClusterRegistry;
import com.jivesoftware.os.miru.manage.deployable.MiruSoyRenderer;
import com.jivesoftware.os.miru.manage.deployable.region.bean.WALBean;
import com.jivesoftware.os.miru.manage.deployable.region.input.MiruActivityWALRegionInput;
import com.jivesoftware.os.miru.wal.activity.MiruActivityWALReader;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import javax.annotation.Nullable;

/**
 *
 */
public class MiruActivityWALRegion implements MiruPageRegion<MiruActivityWALRegionInput> {

    private static final MetricLogger log = MetricLoggerFactory.getLogger();

    private static final long SLEEP_ON_FAILURE_MILLIS = 10_000;

    private final String template;
    private final MiruSoyRenderer renderer;
    private final MiruClusterRegistry clusterRegistry;
    private final MiruActivityWALReader activityWALReader;

    public MiruActivityWALRegion(String template, MiruSoyRenderer renderer, MiruClusterRegistry clusterRegistry, MiruActivityWALReader activityWALReader) {
        this.template = template;
        this.renderer = renderer;
        this.clusterRegistry = clusterRegistry;
        this.activityWALReader = activityWALReader;
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
                List<MiruPartition> partitionsForTenant = clusterRegistry.getPartitionsForTenant(tenantId);
                List<MiruPartitionId> partitions = Lists.transform(partitionsForTenant, partitionToId);
                data.put("partitions", partitions);

                if (optionalPartitionId.isPresent()) {
                    MiruPartitionId partitionId = optionalPartitionId.get();
                    data.put("partition", partitionId.getId());

                    final List<WALBean> walActivities = Lists.newArrayList();
                    final boolean sip = miruActivityWALRegionInput.getSip().or(false);
                    final int limit = miruActivityWALRegionInput.getLimit().or(100);
                    long afterTimestamp = miruActivityWALRegionInput.getAfterTimestamp().or(0l);
                    final AtomicLong lastTimestamp = new AtomicLong();
                    try {
                        MiruActivityWALReader.StreamMiruActivityWAL stream = new MiruActivityWALReader.StreamMiruActivityWAL() {
                            @Override
                            public boolean stream(long collisionId, MiruPartitionedActivity partitionedActivity, long timestamp) throws Exception {
                                walActivities.add(new WALBean(collisionId, Optional.of(partitionedActivity), timestamp));
                                if (collisionId > lastTimestamp.get()) {
                                    lastTimestamp.set(collisionId);
                                }
                                return walActivities.size() < limit;
                            }
                        };
                        if (sip) {
                            // streamSip is exclusive of the given timestamp, so subtract 1
                            activityWALReader.streamSip(tenantId, partitionId, afterTimestamp - 1, limit, SLEEP_ON_FAILURE_MILLIS, stream);
                        } else {
                            activityWALReader.stream(tenantId, partitionId, afterTimestamp, limit, SLEEP_ON_FAILURE_MILLIS, stream);
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

    private final Function<MiruPartition, MiruPartitionId> partitionToId = new Function<MiruPartition, MiruPartitionId>() {
        @Nullable
        @Override
        public MiruPartitionId apply(@Nullable MiruPartition input) {
            return input != null ? input.coord.partitionId : null;
        }
    };

    @Override
    public String getTitle() {
        return "Activity WAL";
    }
}
