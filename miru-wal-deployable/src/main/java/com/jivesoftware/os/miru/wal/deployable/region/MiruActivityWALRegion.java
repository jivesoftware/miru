package com.jivesoftware.os.miru.wal.deployable.region;

import com.google.common.base.Charsets;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.jivesoftware.os.miru.api.activity.MiruPartitionId;
import com.jivesoftware.os.miru.api.activity.MiruPartitionedActivity;
import com.jivesoftware.os.miru.api.activity.MiruPartitionedActivity.Type;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.api.topology.NamedCursor;
import com.jivesoftware.os.miru.api.wal.AmzaCursor;
import com.jivesoftware.os.miru.api.wal.AmzaSipCursor;
import com.jivesoftware.os.miru.api.wal.MiruActivityWALStatus;
import com.jivesoftware.os.miru.api.wal.MiruCursor;
import com.jivesoftware.os.miru.api.wal.MiruSipCursor;
import com.jivesoftware.os.miru.api.wal.MiruWALClient;
import com.jivesoftware.os.miru.api.wal.MiruWALEntry;
import com.jivesoftware.os.miru.api.wal.RCVSCursor;
import com.jivesoftware.os.miru.api.wal.RCVSSipCursor;
import com.jivesoftware.os.miru.ui.MiruPageRegion;
import com.jivesoftware.os.miru.ui.MiruSoyRenderer;
import com.jivesoftware.os.miru.wal.AmzaWALUtil;
import com.jivesoftware.os.miru.wal.MiruWALDirector;
import com.jivesoftware.os.miru.wal.deployable.region.bean.WALBean;
import com.jivesoftware.os.miru.wal.deployable.region.input.MiruActivityWALRegionInput;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

/**
 *
 */
public class MiruActivityWALRegion implements MiruPageRegion<MiruActivityWALRegionInput> {

    private static final MetricLogger log = MetricLoggerFactory.getLogger();

    private final String template;
    private final MiruSoyRenderer renderer;
    private final AmzaWALUtil amzaWALUtil;
    private final MiruWALDirector<RCVSCursor, RCVSSipCursor> rcvsWALDirector;
    private final MiruWALDirector<AmzaCursor, AmzaSipCursor> amzaWALDirector;

    public MiruActivityWALRegion(String template,
        MiruSoyRenderer renderer,
        AmzaWALUtil amzaWALUtil,
        MiruWALDirector<RCVSCursor, RCVSSipCursor> rcvsWALDirector,
        MiruWALDirector<AmzaCursor, AmzaSipCursor> amzaWALDirector) {
        this.template = template;
        this.renderer = renderer;
        this.amzaWALUtil = amzaWALUtil;
        Preconditions.checkArgument(rcvsWALDirector != null || amzaWALDirector != null);
        this.rcvsWALDirector = rcvsWALDirector;
        this.amzaWALDirector = amzaWALDirector;
    }

    @Override
    public String render(MiruActivityWALRegionInput input) {
        Map<String, Object> data = Maps.newHashMap();

        Optional<MiruTenantId> optionalTenantId = input.getTenantId();
        Optional<MiruPartitionId> optionalPartitionId = input.getPartitionId();

        if (optionalTenantId.isPresent()) {
            MiruTenantId tenantId = optionalTenantId.get();
            data.put("tenant", new String(tenantId.getBytes(), Charsets.UTF_8));

            try {
                SortedMap<Integer, Map<String, String>> rcvsPartitions = getPartitions(tenantId, rcvsWALDirector);
                SortedMap<Integer, Map<String, String>> amzaPartitions = getPartitions(tenantId, amzaWALDirector);

                int minPartitionId = Math.min(firstKey(rcvsPartitions, Integer.MAX_VALUE), firstKey(amzaPartitions, Integer.MAX_VALUE));
                int maxPartitionId = Math.max(lastKey(rcvsPartitions, Integer.MIN_VALUE), lastKey(amzaPartitions, Integer.MIN_VALUE));

                List<PartitionBean> partitionBeans = Lists.newArrayList();
                for (int partitionId = maxPartitionId; partitionId >= minPartitionId; partitionId--) {
                    partitionBeans.add(new PartitionBean(String.valueOf(partitionId), rcvsPartitions.get(partitionId), amzaPartitions.get(partitionId)));
                }

                data.put("partitions", partitionBeans);

                if (optionalPartitionId.isPresent()) {
                    MiruPartitionId partitionId = optionalPartitionId.get();
                    String walType = input.getWALType().or("");
                    data.put("partition", partitionId.getId());
                    data.put("walType", walType);

                    List<WALBean> walActivities = Lists.newArrayList();
                    final boolean sip = input.getSip().or(false);
                    final int limit = input.getLimit().or(100);
                    long afterTimestamp = input.getAfterTimestamp().or(0l);
                    final AtomicLong lastTimestamp = new AtomicLong();
                    try {
                        if (walType.equals("rcvs")) {
                            if (sip) {
                                RCVSSipCursor cursor = new RCVSSipCursor(Type.ACTIVITY.getSort(), afterTimestamp, 0, false);
                                addSipActivities(tenantId, partitionId, walActivities, limit, lastTimestamp, rcvsWALDirector, cursor, s -> s.clockTimestamp);
                            } else {
                                RCVSCursor cursor = new RCVSCursor(MiruPartitionedActivity.Type.ACTIVITY.getSort(), afterTimestamp, false, null);
                                addActivities(tenantId, partitionId, walActivities, limit, lastTimestamp, rcvsWALDirector, cursor, c -> c.activityTimestamp);
                            }
                        } else if (walType.equals("amza")) {
                            if (sip) {
                                AmzaSipCursor cursor = new AmzaSipCursor(
                                    Collections.singletonList(new NamedCursor(amzaWALUtil.getRingMemberName(), afterTimestamp)), false);
                                addSipActivities(tenantId, partitionId, walActivities, limit, lastTimestamp, amzaWALDirector, cursor,
                                    s -> amzaWALUtil.extractCursors(s.cursors).get(amzaWALUtil.getRingMemberName()).id);
                            } else {
                                AmzaCursor cursor = new AmzaCursor(
                                    Collections.singletonList(new NamedCursor(amzaWALUtil.getRingMemberName(), afterTimestamp)), null);
                                addActivities(tenantId, partitionId, walActivities, limit, lastTimestamp, amzaWALDirector, cursor,
                                    c -> amzaWALUtil.extractCursors(c.cursors).get(amzaWALUtil.getRingMemberName()).id);
                            }
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

    private Integer firstKey(SortedMap<Integer, Map<String, String>> partitions, Integer defaultValue) {
        return !partitions.isEmpty() ? partitions.firstKey() : defaultValue;
    }

    private Integer lastKey(SortedMap<Integer, Map<String, String>> partitions, Integer defaultValue) {
        return !partitions.isEmpty() ? partitions.lastKey() : defaultValue;
    }

    private <S extends MiruSipCursor<S>, C extends MiruCursor<C, S>> SortedMap<Integer, Map<String, String>> getPartitions(MiruTenantId tenantId,
        MiruWALDirector<C, S> director) throws Exception {

        if (director == null) {
            return Collections.emptySortedMap();
        }

        MiruPartitionId latestPartitionId = director.getLargestPartitionId(tenantId);

        List<MiruActivityWALStatus> partitionStatuses = Lists.newArrayList();
        if (latestPartitionId != null) {
            for (MiruPartitionId latest = latestPartitionId; latest != null; latest = latest.prev()) {
                partitionStatuses.add(director.getActivityWALStatusForTenant(tenantId, latest));
            }
        }

        SortedMap<Integer, Map<String, String>> partitions = Maps.newTreeMap();
        for (MiruActivityWALStatus status : partitionStatuses) {
            long count = 0;
            for (MiruActivityWALStatus.WriterCount writerCount : status.counts) {
                count += writerCount.count;
            }
            partitions.put(status.partitionId.getId(), ImmutableMap.of(
                "count", String.valueOf(count),
                "begins", String.valueOf(status.begins.size()),
                "ends", String.valueOf(status.ends.size())));
        }

        return partitions;
    }

    private <S extends MiruSipCursor<S>, C extends MiruCursor<C, S>> void addActivities(MiruTenantId tenantId,
        MiruPartitionId partitionId,
        List<WALBean> walActivities,
        int limit,
        AtomicLong lastTimestamp,
        MiruWALDirector<C, S> director,
        C cursor,
        Function<C, Long> extractLastTimestamp) throws Exception {

        MiruWALClient.StreamBatch<MiruWALEntry, C> gopped = director.getActivity(tenantId,
            partitionId,
            cursor,
            limit,
            -1L);
        walActivities.addAll(Lists.transform(gopped.activities,
            input -> new WALBean(input.collisionId, Optional.of(input.activity), input.version)));
        lastTimestamp.set(gopped.cursor != null ? extractLastTimestamp.apply(gopped.cursor) : Long.MAX_VALUE);
    }

    private <S extends MiruSipCursor<S>, C extends MiruCursor<C, S>> void addSipActivities(MiruTenantId tenantId,
        MiruPartitionId partitionId,
        List<WALBean> walActivities,
        int limit,
        AtomicLong lastTimestamp,
        MiruWALDirector<C, S> director,
        S cursor,
        Function<S, Long> extractLastTimestamp) throws Exception {

        MiruWALClient.StreamBatch<MiruWALEntry, S> sipped = director.sipActivity(tenantId,
            partitionId,
            cursor,
            null,
            limit);
        walActivities.addAll(Lists.transform(sipped.activities,
            input -> new WALBean(input.collisionId, Optional.of(input.activity), input.version)));
        lastTimestamp.set(sipped.cursor != null ? extractLastTimestamp.apply(sipped.cursor) : Long.MAX_VALUE);
    }

    public static class PartitionBean {

        private final String id;
        private final Map<String, String> rcvs;
        private final Map<String, String> amza;

        public PartitionBean(String id, Map<String, String> rcvs, Map<String, String> amza) {
            this.id = id;
            this.rcvs = rcvs;
            this.amza = amza;
        }

        public String getId() {
            return id;
        }

        public Map<String, String> getRcvs() {
            return rcvs;
        }

        public Map<String, String> getAmza() {
            return amza;
        }

    }

    @Override
    public String getTitle() {
        return "RCVS Activity WAL";
    }
}
