package com.jivesoftware.os.miru.sync.deployable.region;

import com.google.common.collect.Maps;
import com.jivesoftware.os.miru.sync.deployable.MiruSyncSenders;
import com.jivesoftware.os.miru.ui.MiruPageRegion;
import com.jivesoftware.os.miru.ui.MiruSoyRenderer;
import java.util.Map;

/**
 *
 */
public class MiruStatusRegion implements MiruPageRegion<MiruStatusRegionInput> {

    private final String template;
    private final MiruSoyRenderer renderer;
    private final MiruStatusFocusRegion statusFocusRegion;
    private final MiruSyncSenders<?, ?> syncSenders;

    public MiruStatusRegion(String template, MiruSoyRenderer renderer, MiruStatusFocusRegion statusFocusRegion, MiruSyncSenders<?, ?> syncSenders) {
        this.template = template;
        this.renderer = renderer;
        this.statusFocusRegion = statusFocusRegion;
        this.syncSenders = syncSenders;
    }

    @Override
    public String render(MiruStatusRegionInput input) {
        Map<String, Object> data = Maps.newHashMap();
        data.put("syncspaces", syncSenders.getSyncspaces());
        if (input != null) {
            data.put("syncspaceName", input.syncspaceName);
            data.put("tenant", input.tenantId.toString());
            data.put("statusFocusRegion", statusFocusRegion.render(input));
        }
        return renderer.render(template, data);
    }

    @Override
    public String getTitle() {
        return "Status";
    }
}
