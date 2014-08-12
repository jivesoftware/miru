package com.jivesoftware.os.miru.client;

import com.jivesoftware.os.miru.client.base.MiruBestEffortFailureTolerantClient;
import com.jivesoftware.os.miru.cluster.MiruClusterRegistry;
import com.jivesoftware.os.miru.cluster.MiruHostsPartitionsStorage;
import com.jivesoftware.os.miru.cluster.MiruHostsStorage;
import com.jivesoftware.os.miru.cluster.memory.MiruInMemoryHostsPartitionsStorage;
import com.jivesoftware.os.miru.cluster.memory.MiruInMemoryHostsStorage;
import com.jivesoftware.os.miru.cluster.rcvs.MiruRCVSClusterRegistry;
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
import org.merlin.config.defaults.StringDefault;

public interface MiruClientConfig extends Config {

    @IntDefault(5_000_000)
    Integer getTotalCapacity();

    @IntDefault(10)
    Integer getSendActivitiesThreadPoolSize();

    @IntDefault(10000)
    Integer getSocketTimeoutInMillis();

    @IntDefault(100)
    Integer getMaxConnections();

    // This is a default/override used only by naive in-process implementations. Real implementations should read from an hbase registry.
    @StringDefault("")
    String getDefaultHostAddresses();

    @ClassDefault(MiruBestEffortFailureTolerantClient.class)
    Class<? extends MiruClient> getClientClass();

    // This is only used by naive in-process implementations. Real implementations assume 1 writer per node.
    @StringDefault("")
    String getWriterIds();

    @ClassDefault(MiruInMemoryHostsStorage.class)
    Class<? extends MiruHostsStorage> getHostsStorageClass();

    @ClassDefault(MiruRCVSClusterRegistry.class)
    Class<? extends MiruClusterRegistry> getClusterRegistryClass();

    @ClassDefault(MiruInMemoryHostsPartitionsStorage.class)
    Class<? extends MiruHostsPartitionsStorage> getHostsPartitionsStorageClass();

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

}