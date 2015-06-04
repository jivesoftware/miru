package com.jivesoftware.os.miru.analytics.plugins.metrics;

import com.jivesoftware.os.miru.plugin.Miru;
import com.jivesoftware.os.miru.plugin.MiruProvider;
import com.jivesoftware.os.miru.plugin.plugin.MiruEndpointInjectable;
import com.jivesoftware.os.miru.plugin.plugin.MiruPlugin;
import com.jivesoftware.os.miru.plugin.solution.JsonRemotePartitionReader;
import com.jivesoftware.os.miru.plugin.solution.MiruRemotePartition;
import java.util.Collection;
import java.util.Collections;

/**
 *
 */
public class MetricsPlugin implements MiruPlugin<MetricsEndpoints, MetricsInjectable> {

    @Override
    public Class<MetricsEndpoints> getEndpointsClass() {
        return MetricsEndpoints.class;
    }

    @Override
    public Collection<MiruEndpointInjectable<MetricsInjectable>> getInjectables(MiruProvider<? extends Miru> miruProvider) {
        Metrics metrics = new Metrics();

        return Collections.singletonList(new MiruEndpointInjectable<>(
            MetricsInjectable.class,
            new MetricsInjectable(miruProvider, metrics)
        ));
    }

    @Override
    public Collection<MiruRemotePartition<?, ?, ?>> getRemotePartitions() {
        return Collections.singletonList(new MetricsRemotePartition(new JsonRemotePartitionReader()));
    }
}
