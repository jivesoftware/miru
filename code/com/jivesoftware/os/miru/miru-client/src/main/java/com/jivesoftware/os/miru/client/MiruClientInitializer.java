package com.jivesoftware.os.miru.client;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.jivesoftware.os.miru.client.base.MiruBestEffortFailureTolerantClient;
import com.jivesoftware.os.miru.client.base.MiruHttpActivitySenderProvider;
import com.jivesoftware.os.miru.client.rcvs.MiruRCVSPartitionIdProvider;
import com.jivesoftware.os.miru.cluster.MiruActivityLookupTable;
import com.jivesoftware.os.miru.cluster.MiruClusterRegistry;
import com.jivesoftware.os.miru.cluster.MiruRegistryStore;
import com.jivesoftware.os.miru.cluster.rcvs.MiruRCVSActivityLookupTable;
import com.jivesoftware.os.miru.wal.MiruWALInitializer.MiruWAL;
import com.jivesoftware.os.miru.wal.activity.MiruActivityWALWriter;
import com.jivesoftware.os.miru.wal.activity.MiruWriteToActivityAndSipWAL;
import com.jivesoftware.os.miru.wal.readtracking.MiruReadTrackingWALWriter;
import com.jivesoftware.os.miru.wal.readtracking.MiruWriteToReadTrackingAndSipWAL;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class MiruClientInitializer {

    public MiruClient initialize(MiruClientConfig config,
            MiruClusterRegistry clusterRegistry,
            MiruRegistryStore registryStore,
            MiruWAL miruWAL,
            int writerId) throws Exception {

        ExecutorService sendActivitiesToHostsThreadPool = Executors.newFixedThreadPool(config.getSendActivitiesThreadPoolSize());
        MiruActivitySenderProvider activitySenderProvider = new MiruHttpActivitySenderProvider(config, new ObjectMapper());

        MiruActivityWALWriter activityWALWriter = new MiruWriteToActivityAndSipWAL(miruWAL.getActivityWAL(), miruWAL.getActivitySipWAL());
        MiruReadTrackingWALWriter readTrackingWALWriter = new MiruWriteToReadTrackingAndSipWAL(miruWAL.getReadTrackingWAL(), miruWAL.getReadTrackingSipWAL());
        MiruActivityLookupTable activityLookupTable = new MiruRCVSActivityLookupTable(registryStore.getActivityLookupTable());

        MiruPartitionIdProvider miruPartitionIdProvider = new MiruRCVSPartitionIdProvider(config,
                registryStore.getWriterPartitionRegistry(),
                miruWAL.getActivityWAL(),
                miruWAL.getActivitySipWAL());
        MiruPartitioner miruPartitioner = new MiruPartitioner(writerId, miruPartitionIdProvider, activityWALWriter, readTrackingWALWriter, activityLookupTable);

        return new MiruBestEffortFailureTolerantClient(sendActivitiesToHostsThreadPool, clusterRegistry, activitySenderProvider, miruPartitioner);
    }
}
