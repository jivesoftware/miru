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
        data.put("sizeInMemory", readableSize(hostHeartbeat.sizeInMemory));
        data.put("sizeOnDisk", readableSize(hostHeartbeat.sizeOnDisk));

        return renderer.render(template, data);
    }

    private String readableSize(long sizeInBytes) {
        if (sizeInBytes > 1_000_000) {
            return (sizeInBytes / 1_000_000) + " MB";
        } else if (sizeInBytes > 1_000) {
            return (sizeInBytes / 1_000) + " kB";
        } else {
            return sizeInBytes + " b";
        }
    }
}
