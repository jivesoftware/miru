package com.jivesoftware.os.miru.service;

import com.google.inject.Guice;
import com.google.inject.Injector;
import com.jivesoftware.os.miru.api.MiruHost;
import com.jivesoftware.os.miru.cluster.MiruActivityLookupTable;
import com.jivesoftware.os.miru.cluster.MiruClusterRegistry;
import com.jivesoftware.os.miru.cluster.MiruRegistryInitializer;
import com.jivesoftware.os.miru.reader.MiruHttpClientReaderConfig;
import com.jivesoftware.os.miru.service.schema.MiruSchema;
import com.jivesoftware.os.miru.wal.MiruWALInitializer;
import com.jivesoftware.os.miru.wal.activity.MiruActivityWALReader;
import com.jivesoftware.os.miru.wal.readtracking.MiruReadTrackingWALReader;
import javax.inject.Inject;

import static com.google.common.base.Preconditions.checkNotNull;

public final class MiruServiceInitializer {

    public static MiruServiceInitializer initialize(MiruServiceConfig config,
        MiruRegistryInitializer miruRegistryInitializer,
        MiruWALInitializer miruWALInitializer,
        MiruHost miruHost,
        MiruSchema miruSchema,
        MiruHttpClientReaderConfig miruHttpClientReaderConfig) {

        Injector injector = Guice.createInjector(new MiruServiceInitializerModule(config, miruHost, miruSchema,
            miruRegistryInitializer, miruWALInitializer, miruHttpClientReaderConfig));
        return injector.getInstance(MiruServiceInitializer.class);
    }

    private final MiruService miruService;
    private final MiruClusterRegistry clusterRegistry;
    private final MiruActivityWALReader activityWALReader;
    private final MiruReadTrackingWALReader readTrackingWALReader;
    private final MiruActivityLookupTable activityLookupTable;

    @Inject
    MiruServiceInitializer(
        MiruService miruService,
        MiruClusterRegistry clusterRegistry,
        MiruActivityWALReader activityWALReader,
        MiruReadTrackingWALReader readTrackingWALReader,
        MiruActivityLookupTable activityLookupTable) {
        this.readTrackingWALReader = readTrackingWALReader;
        this.miruService = checkNotNull(miruService);
        this.clusterRegistry = checkNotNull(clusterRegistry);
        this.activityWALReader = checkNotNull(activityWALReader);
        this.activityLookupTable = activityLookupTable;
    }

    public MiruService getMiruService() {
        return miruService;
    }

    public MiruClusterRegistry getClusterRegistry() {
        return clusterRegistry;
    }

    public MiruActivityWALReader getActivityWALReader() {
        return activityWALReader;
    }

    public MiruReadTrackingWALReader getReadTrackingWALReader() {
        return readTrackingWALReader;
    }

    public MiruActivityLookupTable getActivityLookupTable() {
        return activityLookupTable;
    }
}