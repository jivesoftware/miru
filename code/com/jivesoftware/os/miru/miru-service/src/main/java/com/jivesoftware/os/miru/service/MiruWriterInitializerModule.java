package com.jivesoftware.os.miru.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.jivesoftware.os.miru.api.MiruWriter;
import javax.inject.Singleton;

import static com.google.common.base.Preconditions.checkNotNull;

public class MiruWriterInitializerModule extends AbstractModule {

    public final MiruWriterConfig config;
    public final MiruService miruService;

    public MiruWriterInitializerModule(MiruWriterConfig config, MiruService miruService) {
        this.config = checkNotNull(config);
        this.miruService = checkNotNull(miruService);
    }

    @Override
    protected void configure() {
        bind(MiruService.class).toInstance(miruService);
        bind(MiruWriter.class).to(MiruWriterImpl.class);
    }

    @Singleton
    @Provides
    public ObjectMapper provideObjectMapper() {
        return new ObjectMapper();
    }

}