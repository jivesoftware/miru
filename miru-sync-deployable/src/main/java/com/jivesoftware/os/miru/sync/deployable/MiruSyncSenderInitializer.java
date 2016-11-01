package com.jivesoftware.os.miru.sync.deployable;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.jivesoftware.os.amza.api.PartitionClientProvider;
import com.jivesoftware.os.amza.client.aquarium.AmzaClientAquariumProvider;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.api.sync.MiruSyncClient;
import com.jivesoftware.os.miru.api.wal.MiruCursor;
import com.jivesoftware.os.miru.api.wal.MiruSipCursor;
import com.jivesoftware.os.miru.api.wal.MiruWALClient;
import com.jivesoftware.os.miru.api.wal.RCVSCursor;
import java.util.List;
import java.util.concurrent.Executors;

/**
 *
 */
public class MiruSyncSenderInitializer {

    <C extends MiruCursor<C, S>, S extends MiruSipCursor<S>> MiruSyncSender<C, S> initialize(MiruSyncConfig syncConfig,
        AmzaClientAquariumProvider amzaClientAquariumProvider,
        MiruWALClient<C, S> walClient,
        MiruSyncClient syncClient,
        PartitionClientProvider amzaClientProvider,
        ObjectMapper mapper,
        List<MiruTenantId> whitelistTenantIds,
        C defaultCursor,
        Class<C> cursorClass) {
        return new MiruSyncSender<>(amzaClientAquariumProvider,
            syncConfig.getSyncRingStripes(),
            Executors.newCachedThreadPool(),
            syncConfig.getSyncThreadCount(),
            syncConfig.getSyncIntervalMillis(),
            walClient,
            syncClient,
            amzaClientProvider,
            mapper,
            whitelistTenantIds,
            syncConfig.getSyncBatchSize(),
            syncConfig.getForwardSyncDelayMillis(),
            defaultCursor,
            cursorClass);
    }
}
