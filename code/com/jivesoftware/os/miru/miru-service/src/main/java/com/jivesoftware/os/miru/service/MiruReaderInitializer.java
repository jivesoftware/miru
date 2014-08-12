package com.jivesoftware.os.miru.service;

import com.google.common.base.Preconditions;
import com.google.inject.Guice;
import com.google.inject.Inject;
import com.jivesoftware.jive.entitlements.entitlements.api.EntitlementExpressionProvider;
import com.jivesoftware.os.miru.api.MiruReader;

public final class MiruReaderInitializer {

    public static MiruReaderInitializer initialize(
        MiruReaderConfig config,
        MiruService miruService,
        EntitlementExpressionProvider entitlementExpressionProvider) {

        return Guice.createInjector(
            new MiruReaderInitializerModule(config, miruService, entitlementExpressionProvider)
        ).getInstance(MiruReaderInitializer.class);
    }

    private final MiruReader miruReader;

    @Inject
    MiruReaderInitializer(MiruReader miruReader) {
        this.miruReader = Preconditions.checkNotNull(miruReader);
    }

    public MiruReader getMiruReader() {
        return miruReader;
    }

}
