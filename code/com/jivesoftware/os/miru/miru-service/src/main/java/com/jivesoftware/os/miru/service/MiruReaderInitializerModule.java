package com.jivesoftware.os.miru.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.jivesoftware.os.miru.api.MiruReader;
import javax.inject.Singleton;

import static com.google.common.base.Preconditions.checkNotNull;

final class MiruReaderInitializerModule extends AbstractModule {

    public final MiruReaderConfig config;
    public final MiruService miruService;

    public MiruReaderInitializerModule(
        MiruReaderConfig config,
        MiruService miruService) {
        this.config = checkNotNull(config);
        this.miruService = checkNotNull(miruService);
    }

    @Override
    protected void configure() {
        bind(MiruService.class).toInstance(miruService);
        bind(MiruReader.class).to(MiruReaderImpl.class);
    }

    @Singleton
    @Provides
    public ObjectMapper provideObjectMapper() {
        return new ObjectMapper();
    }

}
