package com.jivesoftware.os.miru.analytics.plugins.analytics;

import com.jivesoftware.os.miru.plugin.Miru;
import com.jivesoftware.os.miru.plugin.MiruProvider;
import com.jivesoftware.os.miru.plugin.plugin.MiruEndpointInjectable;
import com.jivesoftware.os.miru.plugin.plugin.MiruPlugin;
import com.jivesoftware.os.miru.plugin.solution.MiruRemotePartition;
import com.jivesoftware.os.miru.plugin.solution.SnappyJsonRemotePartitionReader;
import java.util.Collection;
import java.util.Collections;

/**
 *
 */
public class AnalyticsPlugin implements MiruPlugin<AnalyticsEndpoints, AnalyticsInjectable> {

    @Override
    public Class<AnalyticsEndpoints> getEndpointsClass() {
        return AnalyticsEndpoints.class;
    }

    @Override
    public Collection<MiruEndpointInjectable<AnalyticsInjectable>> getInjectables(MiruProvider<? extends Miru> miruProvider) {
        Analytics analytics = new Analytics();

        return Collections.singletonList(new MiruEndpointInjectable<>(
            AnalyticsInjectable.class,
            new AnalyticsInjectable(miruProvider, analytics)
        ));
    }

    @Override
    public Collection<MiruRemotePartition<?, ?, ?>> getRemotePartitions() {
        return Collections.singletonList(new AnalyticsRemotePartition(new SnappyJsonRemotePartitionReader()));
    }
}
