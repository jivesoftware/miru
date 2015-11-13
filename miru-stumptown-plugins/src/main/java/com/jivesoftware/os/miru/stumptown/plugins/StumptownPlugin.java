package com.jivesoftware.os.miru.stumptown.plugins;

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
public class StumptownPlugin implements MiruPlugin<StumptownEndpoints, StumptownInjectable> {

    @Override
    public Class<StumptownEndpoints> getEndpointsClass() {
        return StumptownEndpoints.class;
    }

    @Override
    public Collection<MiruEndpointInjectable<StumptownInjectable>> getInjectables(MiruProvider<? extends Miru> miruProvider) {

        Stumptown stumptown = new Stumptown(miruProvider);
        return Collections.singletonList(new MiruEndpointInjectable<>(
            StumptownInjectable.class,
            new StumptownInjectable(miruProvider, stumptown)
        ));
    }

    @Override
    public Collection<MiruRemotePartition<?, ?, ?>> getRemotePartitions(MiruProvider<? extends Miru> miruProvider) {
        return Collections.singletonList(new StumptownRemotePartition(
            new JsonRemotePartitionReader(miruProvider.getReaderHttpClient(), miruProvider.getReaderStrategyCache())));
    }
}
