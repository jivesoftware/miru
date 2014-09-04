package com.jivesoftware.os.miru.manage.deployable.region;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.collect.Collections2;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.jivesoftware.os.jive.utils.logger.MetricLogger;
import com.jivesoftware.os.jive.utils.logger.MetricLoggerFactory;
import com.jivesoftware.os.miru.api.MiruHost;
import com.jivesoftware.os.miru.cluster.MiruClusterRegistry;
import com.jivesoftware.os.miru.manage.deployable.MiruSoyRenderer;
import java.util.LinkedHashSet;
import java.util.Map;
import javax.annotation.Nullable;

/**
 *
 */
// soy.miru.page.hostsRegion
public class MiruHostsRegion implements MiruPageRegion<Optional<MiruHost>> {

    private static final MetricLogger log = MetricLoggerFactory.getLogger();

    private final String template;
    private final MiruSoyRenderer renderer;
    private final MiruClusterRegistry clusterRegistry;
    private final MiruHostEntryRegion hostEntryRegion;
    private final MiruHostFocusRegion hostFocusRegion;

    public MiruHostsRegion(String template,
            MiruSoyRenderer renderer,
            MiruClusterRegistry clusterRegistry,
            MiruHostEntryRegion hostEntryRegion,
            MiruHostFocusRegion hostFocusRegion) {
        this.template = template;
        this.renderer = renderer;
        this.clusterRegistry = clusterRegistry;
        this.hostEntryRegion = hostEntryRegion;
        this.hostFocusRegion = hostFocusRegion;
    }

    @Override
    public String render(Optional<MiruHost> optionalHost) {
        Map<String, Object> data = Maps.newHashMap();
        LinkedHashSet<MiruClusterRegistry.HostHeartbeat> hostHeartbeats = Sets.newLinkedHashSet();
        try {
            hostHeartbeats = clusterRegistry.getAllHosts();
        } catch (Exception e) {
            log.error("Unable to retrieve all hosts", e);
        }

        data.put("hosts", Collections2.transform(hostHeartbeats, new Function<MiruClusterRegistry.HostHeartbeat, String>() {
            @Nullable
            @Override
            public String apply(@Nullable MiruClusterRegistry.HostHeartbeat input) {
                return input != null ? hostEntryRegion.render(input) : null;
            }
        }));
        if (optionalHost.isPresent()) {
            data.put("hostFocusRegion", hostFocusRegion.render(optionalHost.get()));
        }

        return renderer.render(template, data);
    }

    @Override
    public String getTitle() {
        return "Hosts";
    }
}
