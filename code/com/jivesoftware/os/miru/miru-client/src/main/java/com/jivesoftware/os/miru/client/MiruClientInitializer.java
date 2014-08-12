package com.jivesoftware.os.miru.client;

import com.google.common.base.Optional;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.jivesoftware.os.miru.cluster.MiruRegistryInitializer;
import com.jivesoftware.os.miru.wal.MiruWALInitializer;

public class MiruClientInitializer {

    public static MiruClient initialize(MiruClientConfig config,
        MiruRegistryInitializer miruRegistryInitializer,
        MiruWALInitializer miruWALInitializer,
        int writerId,
        Optional<Class<? extends MiruClient>> clientClass) throws Exception {

        Injector injector = Guice.createInjector(new MiruClientModule(config,
            miruRegistryInitializer, miruWALInitializer, writerId, clientClass));
        return injector.getInstance(MiruClient.class);
    }
}