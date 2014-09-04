package com.jivesoftware.os.miru.manage.deployable.region;

import com.google.common.base.Charsets;
import com.google.common.base.Function;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.jivesoftware.os.jive.utils.logger.MetricLogger;
import com.jivesoftware.os.jive.utils.logger.MetricLoggerFactory;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.cluster.MiruClusterRegistry;
import com.jivesoftware.os.miru.manage.deployable.MiruSoyRenderer;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;

/**
 *
 */
public class MiruHostEntryRegion implements MiruRegion<MiruClusterRegistry.HostHeartbeat> {

    private static final MetricLogger log = MetricLoggerFactory.getLogger();

    private final String template;
    private final MiruSoyRenderer renderer;
    private final MiruClusterRegistry clusterRegistry;

    public MiruHostEntryRegion(String template, MiruSoyRenderer renderer, MiruClusterRegistry clusterRegistry) {
        this.template = template;
        this.renderer = renderer;
        this.clusterRegistry = clusterRegistry;
    }

    @Override
    public String render(MiruClusterRegistry.HostHeartbeat hostHeartbeat) {
        Map<String, Object> data = Maps.newHashMap();
        List<MiruTenantId> tenantsForHost = Lists.newArrayList();
        try {
            tenantsForHost = clusterRegistry.getTenantsForHost(hostHeartbeat.host);
        } catch (Exception e) {
            log.error("Unable to retrieve tenants for host: " + hostHeartbeat.host.toStringForm(), e);
        }

        data.put("host", hostHeartbeat.host);
        data.put("heartbeat", String.valueOf(System.currentTimeMillis() - hostHeartbeat.heartbeat));
        data.put("sizeInMemory", readableSize(hostHeartbeat.sizeInMemory));
        data.put("sizeOnDisk", readableSize(hostHeartbeat.sizeOnDisk));
        data.put("numTenants", tenantsForHost.size());
        data.put("tenants", Lists.transform(tenantsForHost, new Function<MiruTenantId, String>() {
            @Nullable
            @Override
            public String apply(@Nullable MiruTenantId input) {
                return input != null ? new String(input.getBytes(), Charsets.UTF_8) : null;
            }
        }));

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
