package com.jivesoftware.os.miru.manage.deployable.region;

import com.google.common.collect.Maps;
import com.jivesoftware.os.miru.api.MiruHost;
import com.jivesoftware.os.miru.api.topology.HostHeartbeat;
import com.jivesoftware.os.miru.ui.MiruRegion;
import com.jivesoftware.os.miru.ui.MiruSoyRenderer;
import java.util.Map;

/**
 *
 */
public class MiruHostEntryRegion implements MiruRegion<HostHeartbeat> {

    private final String template;
    private final MiruSoyRenderer renderer;

    public MiruHostEntryRegion(String template, MiruSoyRenderer renderer) {
        this.template = template;
        this.renderer = renderer;
    }

    @Override
    public String render(HostHeartbeat hostHeartbeat) {
        Map<String, Object> data = Maps.newHashMap();
        MiruHost host = hostHeartbeat.host;

        data.put("host", host);
        data.put("heartbeat", String.valueOf(System.currentTimeMillis() - hostHeartbeat.heartbeat));

        return renderer.render(template, data);
    }

}
