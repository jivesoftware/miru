package com.jivesoftware.os.miru.wal.deployable.region;

import com.google.common.base.Charsets;
import com.google.common.base.Optional;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.ui.MiruPageRegion;
import com.jivesoftware.os.miru.ui.MiruSoyRenderer;
import com.jivesoftware.os.miru.wal.MiruWALDirector;
import com.jivesoftware.os.miru.wal.deployable.region.bean.LookupBean;
import com.jivesoftware.os.miru.wal.deployable.region.input.MiruLookupRegionInput;
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
    private final MiruWALDirector<?, ?> miruWALDirector;

    public MiruLookupRegion(String template, MiruSoyRenderer renderer, MiruWALDirector<?, ?> miruWALDirector) {
        this.template = template;
        this.renderer = renderer;
        this.miruWALDirector = miruWALDirector;
    }

    @Override
    public String render(MiruLookupRegionInput miruLookupRegionInput) {
        Map<String, Object> data = Maps.newHashMap();
        Optional<MiruTenantId> optionalTenantId = miruLookupRegionInput.getTenantId();

        if (optionalTenantId.isPresent()) {
            MiruTenantId tenantId = optionalTenantId.get();
            data.put("tenant", new String(tenantId.getBytes(), Charsets.UTF_8));

            try {
                List<LookupBean> lookupActivities = Lists.newArrayList();
                final int limit = miruLookupRegionInput.getLimit().or(100);
                long afterTimestamp = miruLookupRegionInput.getAfterTimestamp().or(0l);
                final AtomicLong lastTimestamp = new AtomicLong();
                try {
                    lookupActivities = Lists.transform(miruWALDirector.lookupActivity(tenantId, afterTimestamp, limit),
                        input -> new LookupBean(input.collisionId, input.entry, input.version));
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
