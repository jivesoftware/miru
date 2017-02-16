package com.jivesoftware.os.miru.stream.plugins.fulltext;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.jivesoftware.os.miru.plugin.Miru;
import com.jivesoftware.os.miru.plugin.MiruProvider;
import com.jivesoftware.os.miru.plugin.plugin.LifecycleMiruPlugin;
import com.jivesoftware.os.miru.plugin.plugin.MiruEndpointInjectable;
import com.jivesoftware.os.miru.plugin.plugin.MiruPlugin;
import com.jivesoftware.os.miru.plugin.solution.FstRemotePartitionReader;
import com.jivesoftware.os.miru.plugin.solution.MiruRemotePartition;
import com.jivesoftware.os.miru.plugin.solution.MiruRemotePartitionReader;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicReference;

/**
 *
 */
public class FullTextPlugin implements MiruPlugin<FullTextEndpoints, FullTextInjectable>, LifecycleMiruPlugin {

    private final AtomicReference<FullTextGatherer> fullTextGatherer = new AtomicReference<>();

    private ScheduledExecutorService executorService;

    @Override
    public void start(MiruProvider<? extends Miru> miruProvider) throws Exception {
        if (executorService == null) {
            FullTextConfig config = miruProvider.getConfig(FullTextConfig.class);
            if (config.getGathererEnabled()) {
                executorService = Executors.newScheduledThreadPool(config.getGathererThreadPoolSize(),
                    new ThreadFactoryBuilder().setNameFormat("fullTextGatherer-%d").build());
                FullTextTermProviders fullTextTermProviders = new FullTextTermProviders();

                Class<? extends FullTextTermProviderInitializer> initializerClass = config.getTermProviderInitializerClass();
                FullTextTermProvider fullTextTermProvider = initializerClass.newInstance().initialize(miruProvider);
                fullTextTermProviders.addProvider(fullTextTermProvider);

                FullTextGatherer fullTextGatherer = new FullTextGatherer(miruProvider, fullTextTermProviders, config.getGathererBatchSize(), executorService);
                miruProvider.addIndexOpenCallback(fullTextGatherer);
                miruProvider.addIndexCommitCallback(fullTextGatherer);
                miruProvider.addIndexCloseCallback(fullTextGatherer);
                this.fullTextGatherer.set(fullTextGatherer);
            }
        }
    }

    @Override
    public void stop(MiruProvider<? extends Miru> miruProvider) {
        if (executorService != null) {
            executorService.shutdownNow();
            executorService = null;
            FullTextGatherer fullTextGatherer = this.fullTextGatherer.get();
            miruProvider.removeIndexOpenCallback(fullTextGatherer);
            miruProvider.removeIndexCommitCallback(fullTextGatherer);
            miruProvider.removeIndexCloseCallback(fullTextGatherer);
        }
    }

    @Override
    public Class<FullTextEndpoints> getEndpointsClass() {
        return FullTextEndpoints.class;
    }

    @Override
    public Collection<MiruEndpointInjectable<FullTextInjectable>> getInjectables(MiruProvider<? extends Miru> miruProvider) {

        FullText fullText = new FullText(miruProvider);
        return Collections.singletonList(new MiruEndpointInjectable<>(
            FullTextInjectable.class,
            new FullTextInjectable(miruProvider, fullText)
        ));
    }

    @Override
    public Collection<MiruRemotePartition<?, ?, ?>> getRemotePartitions(MiruProvider<? extends Miru> miruProvider) {
        MiruRemotePartitionReader remotePartitionReader = new FstRemotePartitionReader(miruProvider.getReaderHttpClient(),
                miruProvider.getReaderStrategyCache(), false);
        return Arrays.asList(new FullTextCustomRemotePartition(remotePartitionReader));
    }
}
