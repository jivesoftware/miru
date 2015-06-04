package com.jivesoftware.os.miru.reco.plugins.trending;

import com.jivesoftware.os.miru.analytics.plugins.analytics.Analytics;
import com.jivesoftware.os.miru.plugin.Miru;
import com.jivesoftware.os.miru.plugin.MiruProvider;
import com.jivesoftware.os.miru.plugin.plugin.MiruEndpointInjectable;
import com.jivesoftware.os.miru.plugin.plugin.MiruPlugin;
import com.jivesoftware.os.miru.plugin.solution.MiruRemotePartition;
import com.jivesoftware.os.miru.plugin.solution.SnappyJsonRemotePartitionReader;
import com.jivesoftware.os.miru.reco.plugins.distincts.Distincts;
import java.util.Collection;
import java.util.Collections;

/**
 *
 */
public class TrendingPlugin implements MiruPlugin<TrendingEndpoints, TrendingInjectable> {

    @Override
    public Class<TrendingEndpoints> getEndpointsClass() {
        return TrendingEndpoints.class;
    }

    @Override
    public Collection<MiruEndpointInjectable<TrendingInjectable>> getInjectables(MiruProvider<? extends Miru> miruProvider) {

        Distincts distincts = new Distincts(miruProvider.getTermComposer());
        Analytics analytics = new Analytics();
        return Collections.singletonList(new MiruEndpointInjectable<>(
            TrendingInjectable.class,
            new TrendingInjectable(miruProvider, distincts, analytics)
        ));
    }

    @Override
    public Collection<MiruRemotePartition<?, ?, ?>> getRemotePartitions() {
        return Collections.singletonList(new TrendingRemotePartition(new SnappyJsonRemotePartitionReader()));
    }
}
