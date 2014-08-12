package com.jivesoftware.os.miru.service;

import com.google.common.base.Preconditions;
import com.google.inject.Guice;
import com.jivesoftware.os.miru.api.MiruWriter;
import javax.inject.Inject;

public class MiruWriterInitializer {

    public static MiruWriterInitializer initialize(
        MiruWriterConfig config,
        MiruService miruService) {

        return Guice.createInjector(
            new MiruWriterInitializerModule(config, miruService)
        ).getInstance(MiruWriterInitializer.class);
    }

    private final MiruWriter miruWriter;

    @Inject
    MiruWriterInitializer(MiruWriter miruWriter) {
        this.miruWriter = Preconditions.checkNotNull(miruWriter);
    }

    public MiruWriter getMiruWriter() {
        return miruWriter;
    }
}