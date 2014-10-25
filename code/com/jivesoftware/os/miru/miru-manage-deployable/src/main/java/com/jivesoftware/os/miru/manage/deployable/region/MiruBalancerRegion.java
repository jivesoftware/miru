package com.jivesoftware.os.miru.manage.deployable.region;

import com.google.common.base.Function;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Collections2;
import com.google.common.collect.HashMultiset;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Maps;
import com.google.common.collect.Multiset;
import com.jivesoftware.os.jive.utils.base.interfaces.CallbackStream;
import com.jivesoftware.os.jive.utils.logger.MetricLogger;
import com.jivesoftware.os.jive.utils.logger.MetricLoggerFactory;
import com.jivesoftware.os.miru.api.MiruHost;
import com.jivesoftware.os.miru.api.MiruPartitionState;
import com.jivesoftware.os.miru.api.MiruTopologyStatus;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.cluster.MiruClusterRegistry;
import com.jivesoftware.os.miru.manage.deployable.MiruSoyRenderer;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;

/**
 *
 */
// soy.miru.page.balancerRegion
public class MiruBalancerRegion implements MiruPageRegion<Void> {

    private static final MetricLogger log = MetricLoggerFactory.getLogger();

    private final String template;
    private final MiruSoyRenderer renderer;
    private final MiruClusterRegistry clusterRegistry;

    public MiruBalancerRegion(String template,
        MiruSoyRenderer renderer,
        MiruClusterRegistry clusterRegistry) {
        this.template = template;
        this.renderer = renderer;
        this.clusterRegistry = clusterRegistry;
    }

    @Override
    public String render(Void input) {
        Map<String, Object> data = Maps.newHashMap();
        try {
            LinkedHashSet<MiruClusterRegistry.HostHeartbeat> hostHeartbeats = clusterRegistry.getAllHosts();

            final ListMultimap<MiruHost, MiruTopologyStatus> topologies = ArrayListMultimap.create();
            List<MiruTenantId> tenantIds = clusterRegistry.allTenantIds();
            clusterRegistry.topologiesForTenants(tenantIds, new CallbackStream<MiruTopologyStatus>() {
                @Override
                public MiruTopologyStatus callback(MiruTopologyStatus status) throws Exception {
                    if (status != null) {
                        topologies.put(status.partition.coord.host, status);
                    }
                    return status;
                }
            });

            data.put("hosts", Collections2.transform(hostHeartbeats, new Function<MiruClusterRegistry.HostHeartbeat, Map<String, String>>() {
                @Override
                public Map<String, String> apply(MiruClusterRegistry.HostHeartbeat input) {
                    List<MiruTopologyStatus> statuses = topologies.get(input.host);
                    Multiset<MiruPartitionState> stateCounts = HashMultiset.create();
                    for (MiruTopologyStatus status : statuses) {
                        stateCounts.add(status.partition.info.state);
                    }
                    return ImmutableMap.<String, String>builder()
                        .put("logicalName", input.host.getLogicalName())
                        .put("port", String.valueOf(input.host.getPort()))
                        .put("numOffline", String.valueOf(stateCounts.count(MiruPartitionState.offline)))
                        .put("numBootstrap", String.valueOf(stateCounts.count(MiruPartitionState.bootstrap)))
                        .put("numRebuilding", String.valueOf(stateCounts.count(MiruPartitionState.rebuilding)))
                        .put("numOnline", String.valueOf(stateCounts.count(MiruPartitionState.online)))
                        .build();
                }
            }));
            data.put("numHosts", String.valueOf(hostHeartbeats.size()));

            int totalWidth = 920;
            int hostWidth = totalWidth / hostHeartbeats.size() - 2;
            data.put("imageWidth", String.valueOf(totalWidth));
            data.put("hostWidth", hostWidth);
        } catch (Exception e) {
            log.error("Unable to retrieve data");
        }

        return renderer.render(template, data);
    }

    @Override
    public String getTitle() {
        return "Hosts";
    }
}
