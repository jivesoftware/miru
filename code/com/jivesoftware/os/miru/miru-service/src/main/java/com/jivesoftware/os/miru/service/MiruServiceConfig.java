package com.jivesoftware.os.miru.service;

import com.jivesoftware.os.miru.cluster.MiruClusterRegistry;
import com.jivesoftware.os.miru.cluster.rcvs.MiruRCVSClusterRegistry;
import com.jivesoftware.os.miru.service.partition.MiruExpectedTenants;
import com.jivesoftware.os.miru.service.partition.cluster.MiruClusterExpectedTenants;
import com.jivesoftware.os.miru.service.runner.MiruRestfulServerConfigRunner;
import com.jivesoftware.os.miru.service.runner.MiruRunner;
import com.jivesoftware.os.miru.service.stream.locator.AbstractIdentifierPartResourceLocator;
import com.jivesoftware.os.miru.service.stream.locator.MiruResourceLocator;
import com.jivesoftware.os.miru.service.stream.locator.MiruTransientResourceLocator;
import com.jivesoftware.os.miru.wal.activity.MiruActivityWALReader;
import com.jivesoftware.os.miru.wal.activity.MiruActivityWALReaderImpl;
import com.jivesoftware.os.miru.wal.activity.MiruActivityWALWriter;
import com.jivesoftware.os.miru.wal.activity.MiruWriteToActivityAndSipWAL;
import com.jivesoftware.os.miru.wal.readtracking.MiruReadTrackingWALReader;
import com.jivesoftware.os.miru.wal.readtracking.MiruReadTrackingWALReaderImpl;
import com.jivesoftware.os.miru.wal.readtracking.MiruReadTrackingWALWriter;
import com.jivesoftware.os.miru.wal.readtracking.MiruWriteToReadTrackingAndSipWAL;
import org.merlin.config.Config;
import org.merlin.config.annotations.Property;
import org.merlin.config.defaults.ClassDefault;
import org.merlin.config.defaults.IntDefault;
import org.merlin.config.defaults.LongDefault;
import org.merlin.config.defaults.StringDefault;

public interface MiruServiceConfig extends Config {

    @IntDefault(8192)
    int getBitsetBufferSize();

    @IntDefault(10)
    int getStreamFactoryExecutorCount();

    @ClassDefault(MiruRestfulServerConfigRunner.class)
    Class<? extends MiruRunner> getRunnerClass();

    // This is only used by naive in-process implementations. Real implementations should use the single service config port.
    @StringDefault("")
    String getRunnerPorts();

    @ClassDefault(MiruRCVSClusterRegistry.class)
    Class<? extends MiruClusterRegistry> getClusterRegistryClass();

    @ClassDefault(MiruClusterExpectedTenants.class)
    Class<? extends MiruExpectedTenants> getExpectedTenantsClass();

    @ClassDefault(MiruActivityWALReaderImpl.class)
    @Property("activityWalReaderClass")
    Class<? extends MiruActivityWALReader> getActivityWALReaderClass();

    @ClassDefault(MiruWriteToActivityAndSipWAL.class)
    @Property("activityWalWriterClass")
    Class<? extends MiruActivityWALWriter> getActivityWALWriterClass();

    @ClassDefault(MiruReadTrackingWALReaderImpl.class)
    @Property("readTrackingWalReaderClass")
    Class<? extends MiruReadTrackingWALReader> getReadTrackingWALReaderClass();

    @ClassDefault(MiruWriteToReadTrackingAndSipWAL.class)
    @Property("readTrackingWalWriterClass")
    Class<? extends MiruReadTrackingWALWriter> getReadTrackingWALWriterClass();

    @ClassDefault(AbstractIdentifierPartResourceLocator.class)
    Class<? extends MiruResourceLocator> getDiskResourceLocatorClass();

    @ClassDefault(AbstractIdentifierPartResourceLocator.class)
    Class<? extends MiruTransientResourceLocator> getTransientResourceLocatorClass();

    @StringDefault("var/lib/miru/data")
    String getDiskResourceLocatorPath();

    @StringDefault("var/lib/miru/transient")
    String getTransientResourceLocatorPath();

    @LongDefault(33554432) // 32 MB
    long getDiskResourceInitialChunkSize();

    @LongDefault(4096) // 4 KB
    long getTransientResourceInitialChunkSize();

    @LongDefault(5000)
    long getHeartbeatIntervalInMillis();

    @LongDefault(5000)
    long getEnsurePartitionsIntervalInMillis();

    @IntDefault(1)
    int getDefaultInitialSolvers();

    @IntDefault(10)
    int getDefaultMaxNumberOfSolvers();

    @LongDefault(100)
    long getDefaultAddAnotherSolverAfterNMillis();

    @LongDefault(3000)
    long getDefaultFailAfterNMillis();

    @IntDefault(1000)
    int getPartitionRebuildBatchSize();

    @LongDefault(5000)
    long getPartitionBootstrapIntervalInMillis();

    @LongDefault(5000)
    long getPartitionRunnableIntervalInMillis();

    @IntDefault(10000)
    int getPartitionAuthzCacheSize();

    @StringDefault("hybrid")
    String getDefaultStorage();

    @IntDefault(1000)
    int getLongTailSolverWindowSize();

    @IntDefault(95)
    int getLongTailSolverPercentile();
}
