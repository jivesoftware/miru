package com.jivesoftware.os.miru.stream.plugins.count;

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
public class DistinctCountPlugin implements MiruPlugin<DistinctCountEndpoints, DistinctCountInjectable> {

    @Override
    public Class<DistinctCountEndpoints> getEndpointsClass() {
        return DistinctCountEndpoints.class;
    }

    @Override
    public Collection<MiruEndpointInjectable<DistinctCountInjectable>> getInjectables(MiruProvider<? extends Miru> miruProvider) {

        DistinctCount distinctCount = new DistinctCount();
        return Collections.singletonList(new MiruEndpointInjectable<>(
            DistinctCountInjectable.class,
            new DistinctCountInjectable(miruProvider, distinctCount)
        ));
    }

    @Override
    public Collection<MiruRemotePartition<?, ?, ?>> getRemotePartitions(MiruProvider<? extends Miru> miruProvider) {
        JsonRemotePartitionReader remotePartitionReader = new JsonRemotePartitionReader(miruProvider.getReaderHttpClient(),
            miruProvider.getReaderStrategyCache());
        return Arrays.asList(new DistinctCountCustomRemotePartition(remotePartitionReader),
            new DistinctCountInboxAllRemotePartition(remotePartitionReader));
    }
}
