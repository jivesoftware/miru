package com.jivesoftware.os.miru.service;

import com.google.common.base.Preconditions;
import com.google.inject.Guice;
import com.jivesoftware.os.miru.api.MiruReader;
import javax.inject.Inject;

public final class MiruReaderInitializer {

    public static MiruReaderInitializer initialize(
        MiruReaderConfig config,
        MiruService miruService) {

        return Guice.createInjector(new MiruReaderInitializerModule(config, miruService)).getInstance(MiruReaderInitializer.class);
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
