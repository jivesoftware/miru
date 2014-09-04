package com.jivesoftware.os.miru.manage.deployable.region;

import com.google.common.base.Charsets;
import com.google.common.base.Optional;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.jivesoftware.os.jive.utils.logger.MetricLogger;
import com.jivesoftware.os.jive.utils.logger.MetricLoggerFactory;
import com.jivesoftware.os.miru.api.activity.MiruPartitionedActivity;
import com.jivesoftware.os.miru.api.base.MiruStreamId;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.manage.deployable.MiruSoyRenderer;
import com.jivesoftware.os.miru.manage.deployable.region.bean.WALBean;
import com.jivesoftware.os.miru.manage.deployable.region.input.MiruReadWALRegionInput;
import com.jivesoftware.os.miru.wal.readtracking.MiruReadTrackingWALReader;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.commons.lang.exception.ExceptionUtils;

/**
 *
 */
public class MiruReadWALRegion implements MiruPageRegion<MiruReadWALRegionInput> {

    private static final MetricLogger log = MetricLoggerFactory.getLogger();

    private final String template;
    private final MiruSoyRenderer renderer;
    private final MiruReadTrackingWALReader readTrackingWALReader;

    public MiruReadWALRegion(String template, MiruSoyRenderer renderer, MiruReadTrackingWALReader readTrackingWALReader) {
        this.template = template;
        this.renderer = renderer;
        this.readTrackingWALReader = readTrackingWALReader;
    }

    @Override
    public String render(MiruReadWALRegionInput miruReadWALRegionInput) {
        Map<String, Object> data = Maps.newHashMap();

        Optional<MiruTenantId> optionalTenantId = miruReadWALRegionInput.getTenantId();
        Optional<String> optionalStreamId = miruReadWALRegionInput.getStreamId();

        if (optionalTenantId.isPresent()) {
            MiruTenantId miruTenantId = optionalTenantId.get();
            data.put("tenant", new String(miruTenantId.getBytes(), Charsets.UTF_8));

            if (optionalStreamId.isPresent()) {
                String streamId = optionalStreamId.get();
                data.put("streamId", streamId);

                boolean sip = miruReadWALRegionInput.getSip().or(false);
                final int limit = miruReadWALRegionInput.getLimit().or(100);
                long afterTimestamp = miruReadWALRegionInput.getAfterTimestamp().or(0l);
                final List<WALBean> walReadEvents = Lists.newArrayList();
                final AtomicLong lastTimestamp = new AtomicLong();

                MiruStreamId miruStreamId = new MiruStreamId(streamId.getBytes(Charsets.UTF_8));

                try {
                    if (sip) {
                        readTrackingWALReader.streamSip(miruTenantId, miruStreamId, afterTimestamp,
                                new MiruReadTrackingWALReader.StreamReadTrackingSipWAL() {
                                    @Override
                                    public boolean stream(long eventId, long timestamp) throws Exception {
                                        walReadEvents.add(new WALBean(timestamp, Optional.<MiruPartitionedActivity>absent(), eventId));
                                        if (timestamp > lastTimestamp.get()) {
                                            lastTimestamp.set(timestamp);
                                        }
                                        return walReadEvents.size() < limit;
                                    }
                                });
                    } else {
                        readTrackingWALReader.stream(miruTenantId, miruStreamId, afterTimestamp,
                                new MiruReadTrackingWALReader.StreamReadTrackingWAL() {
                                    @Override
                                    public boolean stream(long eventId, MiruPartitionedActivity partitionedActivity, long timestamp) throws Exception {
                                        walReadEvents.add(new WALBean(eventId, Optional.of(partitionedActivity), timestamp));
                                        if (eventId > lastTimestamp.get()) {
                                            lastTimestamp.set(eventId);
                                        }
                                        return walReadEvents.size() < limit;
                                    }
                                });
                    }
                } catch (Exception e) {
                    log.error("Failed to read read-tracking WAL", e);
                    data.put("error", ExceptionUtils.getStackTrace(e));
                }

                data.put("sip", sip);
                data.put("limit", limit);
                data.put("afterTimestamp", String.valueOf(afterTimestamp));
                data.put("activities", walReadEvents);
                data.put("nextTimestamp", String.valueOf(lastTimestamp.get() + 1));
            }
        }

        return renderer.render(template, data);
    }

    @Override
    public String getTitle() {
        return "Read WAL";
    }
}
