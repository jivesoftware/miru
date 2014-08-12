package com.jivesoftware.os.miru.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.jivesoftware.jive.entitlements.entitlements.api.EntitlementExpressionProvider;
import com.jivesoftware.os.miru.api.MiruReader;
import javax.annotation.Nullable;
import javax.inject.Singleton;

import static com.google.common.base.Preconditions.checkNotNull;

final class MiruReaderInitializerModule extends AbstractModule {

    public final MiruReaderConfig config;
    public final MiruService miruService;
    @Nullable
    public final EntitlementExpressionProvider entitlementExpressionProvider;

    public MiruReaderInitializerModule(
        MiruReaderConfig config,
        MiruService miruService,
        @Nullable EntitlementExpressionProvider entitlementExpressionProvider) {
        this.config = checkNotNull(config);
        this.miruService = checkNotNull(miruService);
        this.entitlementExpressionProvider = entitlementExpressionProvider;
    }

    @Override
    protected void configure() {
        bind(MiruService.class).toInstance(miruService);
        if (entitlementExpressionProvider != null) {
            bind(EntitlementExpressionProvider.class).toInstance(entitlementExpressionProvider);
        }
        bind(MiruReader.class).to(MiruReaderImpl.class);
    }

    @Singleton
    @Provides
    public ObjectMapper provideObjectMapper() {
        return new ObjectMapper();
    }

}
