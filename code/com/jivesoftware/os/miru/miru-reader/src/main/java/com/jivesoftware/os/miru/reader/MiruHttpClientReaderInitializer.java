package com.jivesoftware.os.miru.reader;

import com.google.inject.Guice;
import com.google.inject.Injector;
import com.jivesoftware.os.miru.api.MiruReader;

public final class MiruHttpClientReaderInitializer {

    private MiruHttpClientReaderInitializer() {
        // Prevent construction
    }

    public static MiruReader initialize(MiruHttpClientReaderConfig readerConfig, MiruHttpClientConfigReaderConfig configReaderConfig) throws Exception {
        Injector injector = Guice.createInjector(new MiruHttpClientReaderModule(readerConfig, configReaderConfig));
        return injector.getInstance(MiruReader.class);
    }

}
