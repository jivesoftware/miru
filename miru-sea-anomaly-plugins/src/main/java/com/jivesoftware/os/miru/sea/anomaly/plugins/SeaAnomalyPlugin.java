package com.jivesoftware.os.miru.sea.anomaly.plugins;

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
public class SeaAnomalyPlugin implements MiruPlugin<SeaAnomalyEndpoints, SeaAnomalyInjectable> {

    @Override
    public Class<SeaAnomalyEndpoints> getEndpointsClass() {
        return SeaAnomalyEndpoints.class;
    }

    @Override
    public Collection<MiruEndpointInjectable<SeaAnomalyInjectable>> getInjectables(MiruProvider<? extends Miru> miruProvider) {

        SeaAnomaly anomaly = new SeaAnomaly(miruProvider);
        return Collections.singletonList(new MiruEndpointInjectable<>(
            SeaAnomalyInjectable.class,
            new SeaAnomalyInjectable(miruProvider, anomaly)
        ));
    }

    @Override
    public Collection<MiruRemotePartition<?, ?, ?>> getRemotePartitions(MiruProvider<? extends Miru> miruProvider) {
        return Collections.singletonList(new SeaAnomalyRemotePartition(
            new JsonRemotePartitionReader(miruProvider.getReaderHttpClient(), miruProvider.getReaderStrategyCache())));
    }
}
