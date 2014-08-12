package com.jivesoftware.os.miru.reader;

import com.google.inject.AbstractModule;
import com.google.inject.name.Names;
import com.jivesoftware.os.miru.api.MiruReader;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static com.google.common.base.Preconditions.checkNotNull;

final class MiruHttpClientReaderModule extends AbstractModule {

    private final MiruHttpClientReaderConfig readerConfig;
    private final MiruHttpClientConfigReaderConfig configReaderConfig;

    MiruHttpClientReaderModule(MiruHttpClientReaderConfig readerConfig, MiruHttpClientConfigReaderConfig configReaderConfig) {
        this.readerConfig = checkNotNull(readerConfig);
        this.configReaderConfig = checkNotNull(configReaderConfig);
    }

    @Override
    protected void configure() {
        bind(ExecutorService.class)
            .annotatedWith(Names.named("miruWarmOfflineHostExecutor"))
            .toInstance(Executors.newFixedThreadPool(readerConfig.getWarmOfflineHostExecutorCount()));
        bind(MiruHttpClientReaderConfig.class).toInstance(readerConfig);
        bind(MiruReader.class).to(readerConfig.getReaderClass());

        bind(MiruHttpClientConfigReaderConfig.class).toInstance(configReaderConfig);
    }
}
