package com.jivesoftware.os.miru.sync.deployable;

/**
 *
 */
public class MiruSyncSenderInitializer {

    /*<C extends MiruCursor<C, S>, S extends MiruSipCursor<S>> MiruSyncSender<C, S> initialize(MiruSyncConfig syncConfig,
        AmzaClientAquariumProvider amzaClientAquariumProvider,
        TimestampedOrderIdProvider orderIdProvider,
        MiruClusterClient clusterClient,
        MiruWALClient<C, S> walClient,
        PartitionClientProvider amzaClientProvider,
        ObjectMapper mapper,
        MiruSyncConfigStorage miruSyncConfigStorage,
        C defaultCursor,
        Class<C> cursorClass) throws Exception {

        ClusterSchemaProvider schemaProvider = new ClusterSchemaProvider(clusterClient, 10_000);
        MiruSyncClient syncClient = new HttpSyncClientInitializer().initialize(syncConfig);

        return new MiruSyncSender<>(amzaClientAquariumProvider,
            orderIdProvider,
            syncConfig.getSyncRingStripes(),
            Executors.newCachedThreadPool(),
            syncConfig.getSyncThreadCount(),
            syncConfig.getSyncIntervalMillis(),
            schemaProvider,
            walClient,
            syncClient,
            amzaClientProvider,
            mapper,
            miruSyncConfigStorage,
            syncConfig.getSyncBatchSize(),
            syncConfig.getForwardSyncDelayMillis(),
            syncConfig.getReverseSyncMaxAgeMillis(),
            defaultCursor,
            cursorClass);
    }*/
}
