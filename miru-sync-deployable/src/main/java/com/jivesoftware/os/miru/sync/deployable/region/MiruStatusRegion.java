package com.jivesoftware.os.miru.sync.deployable.region;

import com.google.common.base.Charsets;
import com.google.common.collect.Maps;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.ui.MiruPageRegion;
import com.jivesoftware.os.miru.ui.MiruSoyRenderer;
import java.util.Map;

/**
 *
 */
public class MiruStatusRegion implements MiruPageRegion<MiruTenantId> {

    private final String template;
    private final MiruSoyRenderer renderer;
    private final MiruStatusFocusRegion statusFocusRegion;

    public MiruStatusRegion(String template, MiruSoyRenderer renderer, MiruStatusFocusRegion statusFocusRegion) {
        this.template = template;
        this.renderer = renderer;
        this.statusFocusRegion = statusFocusRegion;
    }

    @Override
    public String render(MiruTenantId tenantId) {
        Map<String, Object> data = Maps.newHashMap();
        if (tenantId != null) {
            data.put("tenant", tenantId.toString());
            data.put("statusFocusRegion", statusFocusRegion.render(tenantId));
        }
        return renderer.render(template, data);
    }

    @Override
    public String getTitle() {
        return "Status";
    }
}
