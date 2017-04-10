package com.jivesoftware.os.miru.stream.plugins.catwalk;

import com.jivesoftware.os.miru.plugin.Miru;
import com.jivesoftware.os.miru.plugin.MiruProvider;
import com.jivesoftware.os.miru.plugin.plugin.MiruEndpointInjectable;
import com.jivesoftware.os.miru.plugin.plugin.MiruPlugin;
import com.jivesoftware.os.miru.plugin.solution.FstRemotePartitionReader;
import com.jivesoftware.os.miru.plugin.solution.MiruRemotePartition;
import com.jivesoftware.os.miru.stream.plugins.strut.StrutConfig;
import com.jivesoftware.os.routing.bird.shared.BoundedExecutor;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.Executor;

/**
 *
 */
public class CatwalkPlugin implements MiruPlugin<CatwalkEndpoints, CatwalkInjectable> {

    @Override
    public Class<CatwalkEndpoints> getEndpointsClass() {
        return CatwalkEndpoints.class;
    }

    @Override
    public Collection<MiruEndpointInjectable<CatwalkInjectable>> getInjectables(MiruProvider<? extends Miru> miruProvider) {

        StrutConfig config = miruProvider.getConfig(StrutConfig.class);
        Catwalk catwalk = new Catwalk(config.getVerboseLogging());
        Executor catwalkExecutor = BoundedExecutor.newBoundedExecutor(config.getCatwalkSolverPoolSize(), "catwalk-solver");

        return Collections.singletonList(new MiruEndpointInjectable<>(
            CatwalkInjectable.class,
            new CatwalkInjectable(miruProvider,
                catwalk,
                catwalkExecutor,
                config.getCatwalkTopNValuesPerFeature(),
                config.getCatwalkTopNTermsPerNumerator(),
                config.getMaxHeapPressureInBytes())
        ));
    }

    @Override
    public Collection<MiruRemotePartition<?, ?, ?>> getRemotePartitions(MiruProvider<? extends Miru> miruProvider) {
        FstRemotePartitionReader remotePartitionReader = new FstRemotePartitionReader(miruProvider.getReaderHttpClient(),
            miruProvider.getReaderStrategyCache(),
            false);
        return Arrays.asList(new CatwalkRemotePartition(remotePartitionReader));
    }
}
