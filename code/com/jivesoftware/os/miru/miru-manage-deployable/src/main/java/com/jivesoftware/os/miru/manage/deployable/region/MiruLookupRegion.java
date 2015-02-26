package com.jivesoftware.os.miru.manage.deployable.region;

import com.google.common.base.Charsets;
import com.google.common.base.Optional;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.wal.lookup.MiruActivityLookupTable;
import com.jivesoftware.os.miru.wal.lookup.MiruActivityLookupEntry;
import com.jivesoftware.os.miru.manage.deployable.MiruSoyRenderer;
import com.jivesoftware.os.miru.manage.deployable.region.bean.LookupBean;
import com.jivesoftware.os.miru.manage.deployable.region.input.MiruLookupRegionInput;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 *
 */
public class MiruLookupRegion implements MiruPageRegion<MiruLookupRegionInput> {

    private static final MetricLogger log = MetricLoggerFactory.getLogger();

    private final String template;
    private final MiruSoyRenderer renderer;
    private final MiruActivityLookupTable activityLookupTable;

    public MiruLookupRegion(String template, MiruSoyRenderer renderer, MiruActivityLookupTable activityLookupTable) {
        this.template = template;
        this.renderer = renderer;
        this.activityLookupTable = activityLookupTable;
    }

    @Override
    public String render(MiruLookupRegionInput miruLookupRegionInput) {
        Map<String, Object> data = Maps.newHashMap();
        Optional<MiruTenantId> optionalTenantId = miruLookupRegionInput.getTenantId();

        if (optionalTenantId.isPresent()) {
            MiruTenantId tenantId = optionalTenantId.get();
            data.put("tenant", new String(tenantId.getBytes(), Charsets.UTF_8));

            try {
                final List<LookupBean> lookupActivities = Lists.newArrayList();
                final int limit = miruLookupRegionInput.getLimit().or(100);
                long afterTimestamp = miruLookupRegionInput.getAfterTimestamp().or(0l);
                final AtomicLong lastTimestamp = new AtomicLong();
                try {
                    activityLookupTable.stream(tenantId, afterTimestamp, new MiruActivityLookupTable.StreamLookupEntry() {
                        @Override
                        public boolean stream(long activityTimestamp, MiruActivityLookupEntry entry, long version) throws Exception {
                            lookupActivities.add(new LookupBean(activityTimestamp, entry, version));
                            if (activityTimestamp > lastTimestamp.get()) {
                                lastTimestamp.set(activityTimestamp);
                            }
                            return lookupActivities.size() < limit;
                        }
                    });
                } catch (Exception e) {
                    log.error("Failed to read activity WAL", e);
                    data.put("error", e.getMessage());
                }

                data.put("limit", limit);
                data.put("afterTimestamp", String.valueOf(afterTimestamp));
                data.put("activities", lookupActivities);
                data.put("nextTimestamp", String.valueOf(lastTimestamp.get() + 1));
            } catch (Exception e) {
                log.error("Failed to lookup activity for tenant", e);
                data.put("error", e.getMessage());
            }
        }

        return renderer.render(template, data);
    }

    @Override
    public String getTitle() {
        return "Activity Lookup";
    }
}
