package com.jivesoftware.os.miru.service;

import com.google.common.io.Files;
import com.jivesoftware.os.miru.api.MiruBackingStorage;
import com.jivesoftware.os.miru.cluster.MiruClusterRegistry;
import com.jivesoftware.os.miru.cluster.rcvs.MiruRCVSClusterRegistry;
import com.jivesoftware.os.miru.service.partition.MiruExpectedTenants;
import com.jivesoftware.os.miru.service.partition.cluster.MiruClusterExpectedTenants;
import com.jivesoftware.os.miru.service.runner.MiruRestfulServerConfigRunner;
import com.jivesoftware.os.miru.service.runner.MiruRunner;
import com.jivesoftware.os.miru.service.stream.locator.MiruResourceLocator;
import com.jivesoftware.os.miru.service.stream.locator.MiruTempDirectoryResourceLocator;
import com.jivesoftware.os.miru.service.stream.locator.MiruTransientResourceLocator;
import com.jivesoftware.os.miru.wal.activity.MiruActivityWALReader;
import com.jivesoftware.os.miru.wal.activity.MiruActivityWALReaderImpl;
import com.jivesoftware.os.miru.wal.activity.MiruActivityWALWriter;
import com.jivesoftware.os.miru.wal.activity.MiruWriteToActivityAndSipWAL;
import com.jivesoftware.os.miru.wal.readtracking.MiruReadTrackingWALReader;
import com.jivesoftware.os.miru.wal.readtracking.MiruReadTrackingWALReaderImpl;
import com.jivesoftware.os.miru.wal.readtracking.MiruReadTrackingWALWriter;
import com.jivesoftware.os.miru.wal.readtracking.MiruWriteToReadTrackingAndSipWAL;

public class MockMiruServiceConfig implements MiruServiceConfig {

    @Override
    public int getBitsetBufferSize() {
        return 8192;
    }

    @Override
    public int getStreamFactoryExecutorCount() {
        return 10;
    }

    @Override
    public Class<? extends MiruRunner> getRunnerClass() {
        return MiruRestfulServerConfigRunner.class;
    }

    @Override
    public String getRunnerPorts() {
        return "";
    }

    @Override
    public Class<? extends MiruClusterRegistry> getClusterRegistryClass() {
        return MiruRCVSClusterRegistry.class;
    }

    @Override
    public Class<? extends MiruExpectedTenants> getExpectedTenantsClass() {
        return MiruClusterExpectedTenants.class;
    }

    @Override
    public Class<? extends MiruActivityWALReader> getActivityWALReaderClass() {
        return MiruActivityWALReaderImpl.class;
    }

    @Override
    public Class<? extends MiruActivityWALWriter> getActivityWALWriterClass() {
        return MiruWriteToActivityAndSipWAL.class;
    }

    @Override
    public Class<? extends MiruReadTrackingWALReader> getReadTrackingWALReaderClass() {
        return MiruReadTrackingWALReaderImpl.class;
    }

    @Override
    public Class<? extends MiruReadTrackingWALWriter> getReadTrackingWALWriterClass() {
        return MiruWriteToReadTrackingAndSipWAL.class;
    }

    @Override
    public Class<? extends MiruResourceLocator> getDiskResourceLocatorClass() {
        return MiruTempDirectoryResourceLocator.class;
    }

    @Override
    public String getDiskResourceLocatorPath() {
        return Files.createTempDir().getPath().toString();
    }

    @Override
    public long getDiskResourceInitialChunkSize() {
        return 4096;
    }

    @Override
    public Class<? extends MiruTransientResourceLocator> getTransientResourceLocatorClass() {
        return MiruTempDirectoryResourceLocator.class;
    }

    @Override
    public String getTransientResourceLocatorPath() {
        return Files.createTempDir().getPath().toString();
    }

    @Override
    public long getTransientResourceInitialChunkSize() {
        return 4096;
    }

    @Override
    public long getHeartbeatIntervalInMillis() {
        return 5000;
    }

    @Override
    public long getEnsurePartitionsIntervalInMillis() {
        return 5000;
    }

    @Override
    public int getDefaultInitialSolvers() {
        return 2;
    }

    @Override
    public int getDefaultMaxNumberOfSolvers() {
        return 10;
    }

    @Override
    public long getDefaultAddAnotherSolverAfterNMillis() {
        return 100;
    }

    @Override
    public long getDefaultFailAfterNMillis() {
        return 3000;
    }

    @Override
    public long getPartitionBootstrapIntervalInMillis() {
        return 5000;
    }

    @Override
    public long getPartitionRunnableIntervalInMillis() {
        return 5000;
    }

    @Override
    public int getPartitionRebuildBatchSize() {
        return 1000;
    }

    @Override
    public int getPartitionAuthzCacheSize() {
        return 10000;
    }

    @Override
    public String getDefaultStorage() {
        return MiruBackingStorage.memory.name();
    }

    @Override
    public int getLongTailSolverWindowSize() {
        return 1000;
    }

    @Override
    public int getLongTailSolverPercentile() {
        return 95;
    }

    @Override
    public String name() {
        return "miruInMemoryService";
    }

    @Override
    public void applyDefaults() {

    }
}
