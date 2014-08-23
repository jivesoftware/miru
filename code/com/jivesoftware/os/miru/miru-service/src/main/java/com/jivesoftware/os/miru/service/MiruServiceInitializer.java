package com.jivesoftware.os.miru.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.guava.GuavaModule;
import com.jivesoftware.os.jive.utils.http.client.HttpClientFactory;
import com.jivesoftware.os.miru.api.MiruBackingStorage;
import com.jivesoftware.os.miru.api.MiruHost;
import com.jivesoftware.os.miru.api.MiruLifecyle;
import com.jivesoftware.os.miru.cluster.MiruActivityLookupTable;
import com.jivesoftware.os.miru.cluster.MiruClusterRegistry;
import com.jivesoftware.os.miru.cluster.MiruRegistryStore;
import com.jivesoftware.os.miru.cluster.rcvs.MiruRCVSActivityLookupTable;
import com.jivesoftware.os.miru.service.bitmap.MiruBitmaps;
import com.jivesoftware.os.miru.service.partition.MiruExpectedTenants;
import com.jivesoftware.os.miru.service.partition.MiruHostedPartitionComparison;
import com.jivesoftware.os.miru.service.partition.MiruLocalPartitionFactory;
import com.jivesoftware.os.miru.service.partition.MiruPartitionDirector;
import com.jivesoftware.os.miru.service.partition.MiruPartitionEventHandler;
import com.jivesoftware.os.miru.service.partition.MiruPartitionInfoProvider;
import com.jivesoftware.os.miru.service.partition.MiruRemotePartitionFactory;
import com.jivesoftware.os.miru.service.partition.MiruTenantTopologyFactory;
import com.jivesoftware.os.miru.service.partition.cluster.CachedClusterPartitionInfoProvider;
import com.jivesoftware.os.miru.service.partition.cluster.MiruClusterExpectedTenants;
import com.jivesoftware.os.miru.api.activity.schema.MiruSchema;
import com.jivesoftware.os.miru.service.stream.MiruStreamFactory;
import com.jivesoftware.os.miru.service.stream.factory.MiruJustInTimeBackfillerizer;
import com.jivesoftware.os.miru.service.stream.factory.MiruLowestLatencySolver;
import com.jivesoftware.os.miru.service.stream.factory.MiruSolver;
import com.jivesoftware.os.miru.service.stream.locator.MiruResourceLocatorProvider;
import com.jivesoftware.os.miru.wal.MiruWALInitializer.MiruWAL;
import com.jivesoftware.os.miru.wal.activity.MiruActivityWALReader;
import com.jivesoftware.os.miru.wal.activity.MiruActivityWALReaderImpl;
import com.jivesoftware.os.miru.wal.activity.MiruActivityWALWriter;
import com.jivesoftware.os.miru.wal.activity.MiruWriteToActivityAndSipWAL;
import com.jivesoftware.os.miru.wal.readtracking.MiruReadTrackingWALReader;
import com.jivesoftware.os.miru.wal.readtracking.MiruReadTrackingWALReaderImpl;
import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public final class MiruServiceInitializer<BM> {

    public MiruLifecyle<MiruService> initialize(final MiruServiceConfig config,
            MiruRegistryStore registryStore,
            MiruClusterRegistry clusterRegistry,
            MiruHost miruHost,
            MiruSchema miruSchema,
            MiruWAL wal,
            HttpClientFactory httpClientFactory,
            MiruResourceLocatorProvider resourceLocatorProvider,
            MiruBitmaps<BM> bitmaps) throws IOException {

        final ScheduledExecutorService scheduledExecutor = Executors.newScheduledThreadPool(6);//TODO expose to config

        MiruHostedPartitionComparison partitionComparison = new MiruHostedPartitionComparison(
                config.getLongTailSolverWindowSize(),
                config.getLongTailSolverPercentile());

        MiruReadTrackingWALReader readTrackingWALReader = new MiruReadTrackingWALReaderImpl(wal.getReadTrackingWAL(), wal.getReadTrackingSipWAL());

        final ExecutorService streamFactoryExecutor = Executors.newFixedThreadPool(config.getStreamFactoryExecutorCount());
        MiruStreamFactory<BM> streamFactory = new MiruStreamFactory<>(bitmaps,
                miruSchema,
                streamFactoryExecutor,
                readTrackingWALReader,
                resourceLocatorProvider.getDiskResourceLocator(),
                resourceLocatorProvider.getTransientResourceLocator(),
                config.getPartitionAuthzCacheSize(),
                MiruBackingStorage.valueOf(config.getDefaultStorage()));

        MiruActivityWALReader activityWALReader = new MiruActivityWALReaderImpl(wal.getActivityWAL(), wal.getActivitySipWAL());
        MiruPartitionEventHandler partitionEventHandler = new MiruPartitionEventHandler(clusterRegistry);
        ObjectMapper objectMapper = new ObjectMapper();
        objectMapper.registerModule(new GuavaModule());

        MiruPartitionInfoProvider partitionInfoProvider = new CachedClusterPartitionInfoProvider();

        MiruLocalPartitionFactory localPartitionFactory = new MiruLocalPartitionFactory(config,
                streamFactory,
                activityWALReader,
                partitionEventHandler,
                scheduledExecutor);

        MiruRemotePartitionFactory remotePartitionFactory = new MiruRemotePartitionFactory(partitionInfoProvider,
                httpClientFactory,
                objectMapper);

        MiruTenantTopologyFactory tenantTopologyFactory = new MiruTenantTopologyFactory(config,
                miruHost,
                localPartitionFactory,
                remotePartitionFactory,
                partitionComparison);

        MiruExpectedTenants expectedTenants = new MiruClusterExpectedTenants(partitionInfoProvider,
                tenantTopologyFactory,
                clusterRegistry);

        final MiruPartitionDirector partitionDirector = new MiruPartitionDirector(miruHost, clusterRegistry, expectedTenants);
        MiruActivityWALWriter activityWALWriter = new MiruWriteToActivityAndSipWAL(wal.getActivityWAL(), wal.getActivitySipWAL());
        MiruActivityLookupTable activityLookupTable = new MiruRCVSActivityLookupTable(registryStore.getActivityLookupTable());

        final ExecutorService serviceExecutor = Executors.newFixedThreadPool(10); //TODO expose to config
        MiruSolver solver = new MiruLowestLatencySolver(serviceExecutor,
                config.getDefaultInitialSolvers(),
                config.getDefaultMaxNumberOfSolvers(),
                config.getDefaultAddAnotherSolverAfterNMillis(),
                config.getDefaultFailAfterNMillis());

        final ExecutorService backfillExecutor = Executors.newFixedThreadPool(10);//TODO expose to config
        MiruJustInTimeBackfillerizer<BM> backfillerizer = new MiruJustInTimeBackfillerizer<>(miruHost, bitmaps, backfillExecutor);

        final MiruService<BM> miruService = new MiruService<>(
                miruHost,
                serviceExecutor,
                scheduledExecutor,
                backfillerizer,
                partitionDirector,
                partitionComparison,
                activityWALWriter,
                activityLookupTable,
                solver,
                bitmaps);

        return new MiruLifecyle() {

            @Override
            public Object getService() {
                return miruService;
            }

            @Override
            public void start() throws Exception {
                long heartbeatInterval = config.getHeartbeatIntervalInMillis();
                scheduledExecutor.scheduleWithFixedDelay(new Runnable() {

                    @Override
                    public void run() {
                        partitionDirector.heartbeat();
                    }
                }, 0, heartbeatInterval, TimeUnit.MILLISECONDS);
                long ensurePartitionsInterval = config.getEnsurePartitionsIntervalInMillis();
                scheduledExecutor.scheduleWithFixedDelay(new Runnable() {

                    @Override
                    public void run() {
                        partitionDirector.ensureServerPartitions();
                    }
                }, 0, ensurePartitionsInterval, TimeUnit.MILLISECONDS);
            }

            @Override
            public void stop() throws Exception {
                serviceExecutor.shutdownNow();
                scheduledExecutor.shutdownNow();
                backfillExecutor.shutdownNow();
                streamFactoryExecutor.shutdownNow();
            }
        };
    }

}
