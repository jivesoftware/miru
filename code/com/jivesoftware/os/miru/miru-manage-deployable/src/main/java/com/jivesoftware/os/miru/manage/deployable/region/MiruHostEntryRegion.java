package com.jivesoftware.os.miru.manage.deployable.region;

import com.google.common.collect.Maps;
import com.jivesoftware.os.miru.api.MiruHost;
import com.jivesoftware.os.miru.cluster.MiruClusterRegistry;
import com.jivesoftware.os.miru.manage.deployable.MiruSoyRenderer;
import java.util.Map;

/**
 *
 */
public class MiruHostEntryRegion implements MiruRegion<MiruClusterRegistry.HostHeartbeat> {

    private final String template;
    private final MiruSoyRenderer renderer;

    public MiruHostEntryRegion(String template, MiruSoyRenderer renderer) {
        this.template = template;
        this.renderer = renderer;
    }

    @Override
    public String render(MiruClusterRegistry.HostHeartbeat hostHeartbeat) {
        Map<String, Object> data = Maps.newHashMap();
        MiruHost host = hostHeartbeat.host;

        data.put("host", host);
        data.put("heartbeat", String.valueOf(System.currentTimeMillis() - hostHeartbeat.heartbeat));

        return renderer.render(template, data);
    }

}
