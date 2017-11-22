package com.jivesoftware.os.miru.stream.plugins.filter;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.jivesoftware.os.miru.api.base.MiruStreamId;
import com.jivesoftware.os.miru.plugin.Miru;
import com.jivesoftware.os.miru.plugin.MiruProvider;
import com.jivesoftware.os.miru.plugin.plugin.MiruEndpointInjectable;
import com.jivesoftware.os.miru.plugin.plugin.MiruPlugin;
import com.jivesoftware.os.miru.plugin.solution.JsonRemotePartitionReader;
import com.jivesoftware.os.miru.plugin.solution.MiruRemotePartition;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Set;

public class AggregateCountsPlugin implements MiruPlugin<AggregateCountsEndpoints, AggregateCountsInjectable> {
    @Override
    public Class<AggregateCountsEndpoints> getEndpointsClass() {
        return AggregateCountsEndpoints.class;
    }

    @Override
    public Collection<MiruEndpointInjectable<AggregateCountsInjectable>> getInjectables(MiruProvider<? extends Miru> miruProvider) {
        AggregateCountsConfig config = miruProvider.getConfig(AggregateCountsConfig.class);
        AggregateCounts aggregateCounts = new AggregateCounts();

        boolean verboseAllStreamIds = config.getVerboseStreamIds().trim().equals("*");
        Set<MiruStreamId> verboseStreamIds = Sets.newHashSet(Lists.transform(
            Arrays.asList(config.getVerboseStreamIds().split("\\s*,\\s*")),
            input -> new MiruStreamId(input.getBytes(StandardCharsets.UTF_8))));
        return Collections.singletonList(new MiruEndpointInjectable<>(
            AggregateCountsInjectable.class,
            new AggregateCountsInjectable(miruProvider, aggregateCounts, verboseStreamIds, verboseAllStreamIds)
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
