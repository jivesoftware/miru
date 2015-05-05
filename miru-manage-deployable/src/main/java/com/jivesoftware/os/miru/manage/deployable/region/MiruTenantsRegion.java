package com.jivesoftware.os.miru.manage.deployable.region;

import com.google.common.base.Charsets;
import com.google.common.base.Optional;
import com.google.common.collect.Maps;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.manage.deployable.MiruSoyRenderer;
import java.util.Map;

/**
 *
 */
public class MiruTenantsRegion implements MiruPageRegion<Optional<MiruTenantId>> {

    private final String template;
    private final MiruSoyRenderer renderer;
    private final MiruTenantEntryRegion tenantEntryRegion;

    public MiruTenantsRegion(String template, MiruSoyRenderer renderer, MiruTenantEntryRegion tenantEntryRegion) {
        this.template = template;
        this.renderer = renderer;
        this.tenantEntryRegion = tenantEntryRegion;
    }

    @Override
    public String render(Optional<MiruTenantId> optionalTenantId) {
        Map<String, Object> data = Maps.newHashMap();
        if (optionalTenantId.isPresent()) {
            MiruTenantId tenantId = optionalTenantId.get();
            data.put("tenant", new String(tenantId.getBytes(), Charsets.UTF_8));
            data.put("tenantEntryRegion", tenantEntryRegion.render(tenantId));
        }
        return renderer.render(template, data);
    }

    @Override
    public String getTitle() {
        return "Tenants";
    }
}
