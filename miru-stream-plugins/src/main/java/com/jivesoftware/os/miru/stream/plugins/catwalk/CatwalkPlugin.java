package com.jivesoftware.os.miru.stream.plugins.catwalk;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.jivesoftware.os.miru.plugin.Miru;
import com.jivesoftware.os.miru.plugin.MiruProvider;
import com.jivesoftware.os.miru.plugin.plugin.MiruEndpointInjectable;
import com.jivesoftware.os.miru.plugin.plugin.MiruPlugin;
import com.jivesoftware.os.miru.plugin.solution.FstRemotePartitionReader;
import com.jivesoftware.os.miru.plugin.solution.MiruRemotePartition;
import com.jivesoftware.os.miru.stream.plugins.strut.StrutConfig;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.Executor;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

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
        Executor catwalkExecutor = new ThreadPoolExecutor(config.getCatwalkSolverPoolSize(), config.getCatwalkSolverPoolSize(),
            60L, TimeUnit.SECONDS,
            new LinkedBlockingQueue<>(),
            new ThreadFactoryBuilder().setNameFormat("catwalk-solver-%d").build());

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
