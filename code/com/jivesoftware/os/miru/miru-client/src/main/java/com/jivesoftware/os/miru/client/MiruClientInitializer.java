package com.jivesoftware.os.miru.client;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;
import com.jivesoftware.os.jive.utils.http.client.HttpClientConfig;
import com.jivesoftware.os.jive.utils.http.client.HttpClientConfiguration;
import com.jivesoftware.os.jive.utils.http.client.HttpClientFactory;
import com.jivesoftware.os.jive.utils.http.client.HttpClientFactoryProvider;
import com.jivesoftware.os.miru.api.topology.MiruClusterClient;
import com.jivesoftware.os.miru.client.base.MiruBestEffortFailureTolerantClient;
import com.jivesoftware.os.miru.client.base.MiruLiveIngressActivitySenderProvider;
import com.jivesoftware.os.miru.client.base.MiruWarmActivitySenderProvider;
import com.jivesoftware.os.miru.cluster.client.MiruReplicaSetDirector;
import com.jivesoftware.os.miru.wal.MiruWALInitializer.MiruWAL;
import com.jivesoftware.os.miru.wal.activity.MiruActivityWALReader;
import com.jivesoftware.os.miru.wal.activity.MiruActivityWALReaderImpl;
import com.jivesoftware.os.miru.wal.activity.MiruActivityWALWriter;
import com.jivesoftware.os.miru.wal.activity.MiruWriteToActivityAndSipWAL;
import com.jivesoftware.os.miru.wal.lookup.MiruActivityLookupTable;
import com.jivesoftware.os.miru.wal.lookup.MiruRCVSActivityLookupTable;
import com.jivesoftware.os.miru.wal.partition.MiruPartitionIdProvider;
import com.jivesoftware.os.miru.wal.partition.MiruRCVSPartitionIdProvider;
import com.jivesoftware.os.miru.wal.readtracking.MiruReadTrackingWALWriter;
import com.jivesoftware.os.miru.wal.readtracking.MiruWriteToReadTrackingAndSipWAL;
import java.util.Collection;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class MiruClientInitializer {

    public MiruClient initialize(MiruClientConfig config,
        MiruClusterClient clusterClient,
        MiruReplicaSetDirector replicaSetDirector,
        MiruWAL miruWAL,
        int writerId) throws Exception {

        ExecutorService sendActivitiesToHostsThreadPool = Executors.newFixedThreadPool(config.getSendActivitiesThreadPoolSize());

        Collection<HttpClientConfiguration> configurations = Lists.newArrayList();
        HttpClientConfig baseConfig = HttpClientConfig.newBuilder() // TODO refactor so this is passed in.
            .setSocketTimeoutInMillis(config.getSocketTimeoutInMillis())
            .setMaxConnections(config.getMaxConnections())
            .build();
        configurations.add(baseConfig);
        HttpClientFactory httpClientFactory = new HttpClientFactoryProvider().createHttpClientFactory(configurations);

        MiruActivitySenderProvider activitySenderProvider;
        if (config.getLiveIngress()) {
            activitySenderProvider = new MiruLiveIngressActivitySenderProvider(httpClientFactory, new ObjectMapper());
        } else {
            activitySenderProvider = new MiruWarmActivitySenderProvider(httpClientFactory, new ObjectMapper());
        }

        MiruActivityWALWriter activityWALWriter = new MiruWriteToActivityAndSipWAL(miruWAL.getActivityWAL(), miruWAL.getActivitySipWAL());
        MiruActivityWALReader activityWALReader = new MiruActivityWALReaderImpl(miruWAL.getActivityWAL(), miruWAL.getActivitySipWAL());
        MiruReadTrackingWALWriter readTrackingWALWriter = new MiruWriteToReadTrackingAndSipWAL(miruWAL.getReadTrackingWAL(), miruWAL.getReadTrackingSipWAL());
        MiruActivityLookupTable activityLookupTable = new MiruRCVSActivityLookupTable(miruWAL.getActivityLookupTable());

        MiruPartitionIdProvider miruPartitionIdProvider = new MiruRCVSPartitionIdProvider(config.getTotalCapacity(),
            miruWAL.getWriterPartitionRegistry(),
            miruWAL.getActivitySipWAL());

        MiruPartitioner miruPartitioner = new MiruPartitioner(writerId,
            miruPartitionIdProvider,
            activityWALWriter,
            activityWALReader,
            readTrackingWALWriter,
            activityLookupTable,
            config.getPartitionMaximumAgeInMillis());

        return new MiruBestEffortFailureTolerantClient(sendActivitiesToHostsThreadPool,
            clusterClient,
            replicaSetDirector,
            activitySenderProvider,
            miruPartitioner,
            config.getTopologyCacheSize(),
            config.getTopologyCacheExpiresInMillis()
        );
    }
}
