package com.jivesoftware.os.miru.service;

import com.google.inject.AbstractModule;
import com.jivesoftware.os.miru.api.MiruReader;
import com.jivesoftware.os.miru.api.MiruWriter;

public class MiruReaderWriterInMemoryModule extends AbstractModule {

    @Override
    protected void configure() {
        install(new MiruServiceInMemoryModule());
        bind(MiruReader.class).to(MiruReaderImpl.class);
        bind(MiruWriter.class).to(MiruWriterImpl.class);
    }

}
