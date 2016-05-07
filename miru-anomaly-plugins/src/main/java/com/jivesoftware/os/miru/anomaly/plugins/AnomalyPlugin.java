package com.jivesoftware.os.miru.anomaly.plugins;

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
public class AnomalyPlugin implements MiruPlugin<AnomalyEndpoints, AnomalyInjectable> {

    @Override
    public Class<AnomalyEndpoints> getEndpointsClass() {
        return AnomalyEndpoints.class;
    }

    @Override
    public Collection<MiruEndpointInjectable<AnomalyInjectable>> getInjectables(MiruProvider<? extends Miru> miruProvider) {

        Anomaly anomaly = new Anomaly();
        return Collections.singletonList(new MiruEndpointInjectable<>(
            AnomalyInjectable.class,
            new AnomalyInjectable(miruProvider, anomaly)
        ));
    }

    @Override
    public Collection<MiruRemotePartition<?, ?, ?>> getRemotePartitions(MiruProvider<? extends Miru> miruProvider) {
        return Collections.singletonList(new AnomalyRemotePartition(
            new JsonRemotePartitionReader(miruProvider.getReaderHttpClient(), miruProvider.getReaderStrategyCache())));
    }
}
