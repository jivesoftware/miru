package com.jivesoftware.os.miru.sync.deployable;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.jivesoftware.os.amza.api.PartitionClientProvider;
import com.jivesoftware.os.amza.client.aquarium.AmzaClientAquariumProvider;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.api.sync.MiruSyncClient;
import com.jivesoftware.os.miru.api.topology.MiruClusterClient;
import com.jivesoftware.os.miru.api.wal.MiruCursor;
import com.jivesoftware.os.miru.api.wal.MiruSipCursor;
import com.jivesoftware.os.miru.api.wal.MiruWALClient;
import com.jivesoftware.os.miru.cluster.client.ClusterSchemaProvider;
import java.util.Map;
import java.util.concurrent.Executors;

/**
 *
 */
public class MiruSyncSenderInitializer {

    <C extends MiruCursor<C, S>, S extends MiruSipCursor<S>> MiruSyncSender<C, S> initialize(MiruSyncConfig syncConfig,
        AmzaClientAquariumProvider amzaClientAquariumProvider,
        MiruClusterClient clusterClient,
        MiruWALClient<C, S> walClient,
        PartitionClientProvider amzaClientProvider,
        ObjectMapper mapper,
        Map<MiruTenantId, MiruTenantId> whitelistTenantIds,
        C defaultCursor,
        Class<C> cursorClass) throws Exception {

        ClusterSchemaProvider schemaProvider = new ClusterSchemaProvider(clusterClient, 10_000);
        MiruSyncClient syncClient = new HttpSyncClientInitializer().initialize(syncConfig);

        return new MiruSyncSender<>(amzaClientAquariumProvider,
            syncConfig.getSyncRingStripes(),
            Executors.newCachedThreadPool(),
            syncConfig.getSyncThreadCount(),
            syncConfig.getSyncIntervalMillis(),
            schemaProvider,
            walClient,
            syncClient,
            amzaClientProvider,
            mapper,
            whitelistTenantIds,
            syncConfig.getSyncBatchSize(),
            syncConfig.getForwardSyncDelayMillis(),
            syncConfig.getReverseSyncMaxAgeMillis(),
            defaultCursor,
            cursorClass);
    }
}
