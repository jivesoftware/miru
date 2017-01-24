package com.jivesoftware.os.miru.stream.plugins.filter;

import com.jivesoftware.os.miru.plugin.Miru;
import com.jivesoftware.os.miru.plugin.MiruProvider;
import com.jivesoftware.os.miru.plugin.plugin.MiruEndpointInjectable;
import com.jivesoftware.os.miru.plugin.plugin.MiruPlugin;
import com.jivesoftware.os.miru.plugin.solution.JsonRemotePartitionReader;
import com.jivesoftware.os.miru.plugin.solution.MiruRemotePartition;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;

/**
 *
 */
public class AggregateCountsPlugin implements MiruPlugin<AggregateCountsEndpoints, AggregateCountsInjectable> {

    @Override
    public Class<AggregateCountsEndpoints> getEndpointsClass() {
        return AggregateCountsEndpoints.class;
    }

    @Override
    public Collection<MiruEndpointInjectable<AggregateCountsInjectable>> getInjectables(MiruProvider<? extends Miru> miruProvider) {

        AggregateCounts aggregateCounts = new AggregateCounts();
        return Collections.singletonList(new MiruEndpointInjectable<>(
            AggregateCountsInjectable.class,
            new AggregateCountsInjectable(miruProvider, aggregateCounts)
        ));
    }

    @Override
    public Collection<MiruRemotePartition<?, ?, ?>> getRemotePartitions(MiruProvider<? extends Miru> miruProvider) {
        JsonRemotePartitionReader remotePartitionReader = new JsonRemotePartitionReader(miruProvider.getReaderHttpClient(),
            miruProvider.getReaderStrategyCache());
        return Arrays.asList(new AggregateCountsCustomRemotePartition(remotePartitionReader),
            new AggregateCountsInboxAllRemotePartition(remotePartitionReader));
    }
}
