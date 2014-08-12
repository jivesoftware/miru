package com.jivesoftware.os.miru.wal;

import com.google.common.base.Preconditions;
import com.google.inject.Guice;
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.jivesoftware.os.jive.utils.id.SaltingDelegatingMarshaller;
import com.jivesoftware.os.jive.utils.id.TenantId;
import com.jivesoftware.os.jive.utils.id.TenantIdMarshaller;
import com.jivesoftware.os.jive.utils.row.column.value.store.api.DefaultRowColumnValueStoreMarshaller;
import com.jivesoftware.os.jive.utils.row.column.value.store.api.RowColumnValueStore;
import com.jivesoftware.os.jive.utils.row.column.value.store.api.SetOfSortedMapsImplInitializer;
import com.jivesoftware.os.jive.utils.row.column.value.store.api.timestamper.CurrentTimestamper;
import com.jivesoftware.os.jive.utils.row.column.value.store.marshall.primatives.LongTypeMarshaller;
import com.jivesoftware.os.miru.api.activity.MiruPartitionedActivity;
import com.jivesoftware.os.miru.wal.activity.MiruActivityWALReader;
import com.jivesoftware.os.miru.wal.activity.MiruActivityWALWriter;
import com.jivesoftware.os.miru.wal.activity.hbase.MiruActivitySipWALColumnKey;
import com.jivesoftware.os.miru.wal.activity.hbase.MiruActivitySipWALColumnKeyMarshaller;
import com.jivesoftware.os.miru.wal.activity.hbase.MiruActivityWALColumnKey;
import com.jivesoftware.os.miru.wal.activity.hbase.MiruActivityWALColumnKeyMarshaller;
import com.jivesoftware.os.miru.wal.activity.hbase.MiruActivityWALRow;
import com.jivesoftware.os.miru.wal.activity.hbase.MiruActivityWALRowMarshaller;
import com.jivesoftware.os.miru.wal.readtracking.MiruReadTrackingWALReader;
import com.jivesoftware.os.miru.wal.readtracking.MiruReadTrackingWALWriter;
import com.jivesoftware.os.miru.wal.readtracking.hbase.MiruReadTrackingSipWALColumnKey;
import com.jivesoftware.os.miru.wal.readtracking.hbase.MiruReadTrackingSipWALColumnKeyMarshaller;
import com.jivesoftware.os.miru.wal.readtracking.hbase.MiruReadTrackingWALColumnKey;
import com.jivesoftware.os.miru.wal.readtracking.hbase.MiruReadTrackingWALColumnKeyMarshaller;
import com.jivesoftware.os.miru.wal.readtracking.hbase.MiruReadTrackingWALRow;
import com.jivesoftware.os.miru.wal.readtracking.hbase.MiruReadTrackingWALRowMarshaller;

public class MiruWALInitializer {

    public static MiruWALInitializer initialize(String tableNameSpace,
        SetOfSortedMapsImplInitializer<? extends Exception> setOfSortedMapsImplInitializer,
        Class<? extends MiruActivityWALReader> activityWALReaderClass,
        Class<? extends MiruActivityWALWriter> activityWALWriterClass,
        Class<? extends MiruReadTrackingWALReader> readTrackingWALReaderClass,
        Class<? extends MiruReadTrackingWALWriter> readTrackingWALWriterClass) throws Exception {

        // Miru ActivityWAL
        RowColumnValueStore<TenantId, MiruActivityWALRow, MiruActivityWALColumnKey, MiruPartitionedActivity, ? extends Exception> activityWAL =
            setOfSortedMapsImplInitializer.initialize(
                tableNameSpace,
                "miru.activity.wal", "a", new String[] { "s" }, new DefaultRowColumnValueStoreMarshaller<>(
                            new TenantIdMarshaller(),
                            new SaltingDelegatingMarshaller<>(new MiruActivityWALRowMarshaller()),
                            new MiruActivityWALColumnKeyMarshaller(),
                            new JacksonJsonObjectTypeMarshaller<>(MiruPartitionedActivity.class)), new CurrentTimestamper()
            );

        // Miru ActivitySipWAL
        RowColumnValueStore<TenantId, MiruActivityWALRow, MiruActivitySipWALColumnKey, MiruPartitionedActivity, ? extends Exception> activitySipWAL =
            setOfSortedMapsImplInitializer.initialize(
                tableNameSpace,
                "miru.activity.wal", "s", new DefaultRowColumnValueStoreMarshaller<>(
                            new TenantIdMarshaller(),
                            new SaltingDelegatingMarshaller<>(new MiruActivityWALRowMarshaller()),
                            new MiruActivitySipWALColumnKeyMarshaller(),
                            new JacksonJsonObjectTypeMarshaller<>(MiruPartitionedActivity.class)), new CurrentTimestamper()
            );

        // Miru ReadTrackingWAL
        RowColumnValueStore<TenantId, MiruReadTrackingWALRow, MiruReadTrackingWALColumnKey, MiruPartitionedActivity, ? extends Exception> readTrackingWAL =
            setOfSortedMapsImplInitializer.initialize(
                tableNameSpace,
                "miru.readtracking.wal", "r", new String[] { "s" }, new DefaultRowColumnValueStoreMarshaller<>(
                            new TenantIdMarshaller(),
                            new SaltingDelegatingMarshaller<>(new MiruReadTrackingWALRowMarshaller()),
                            new MiruReadTrackingWALColumnKeyMarshaller(),
                            new JacksonJsonObjectTypeMarshaller<>(MiruPartitionedActivity.class)), new CurrentTimestamper()
            );

        // Miru ReadTrackingSipWAL
        RowColumnValueStore<TenantId, MiruReadTrackingWALRow, MiruReadTrackingSipWALColumnKey, Long, ? extends Exception> readTrackingSipWAL =
            setOfSortedMapsImplInitializer.initialize(
                tableNameSpace,
                "miru.readtracking.wal", "s", new DefaultRowColumnValueStoreMarshaller<>(
                            new TenantIdMarshaller(),
                            new SaltingDelegatingMarshaller<>(new MiruReadTrackingWALRowMarshaller()),
                            new MiruReadTrackingSipWALColumnKeyMarshaller(),
                            new LongTypeMarshaller()), new CurrentTimestamper()
            );

        Injector injector = Guice.createInjector(new MiruWALInitializerModule(activityWAL, activitySipWAL, readTrackingWAL, readTrackingSipWAL,
            activityWALReaderClass, activityWALWriterClass, readTrackingWALReaderClass, readTrackingWALWriterClass));
        return injector.getInstance(MiruWALInitializer.class);
    }

    private final RowColumnValueStore<TenantId, MiruActivityWALRow, MiruActivityWALColumnKey, MiruPartitionedActivity, ? extends Exception>
        activityWAL;
    private final RowColumnValueStore<TenantId, MiruActivityWALRow, MiruActivitySipWALColumnKey, MiruPartitionedActivity, ? extends Exception>
        activitySipWAL;
    private final RowColumnValueStore<TenantId, MiruReadTrackingWALRow, MiruReadTrackingWALColumnKey, MiruPartitionedActivity, ? extends Exception>
        readTrackingWAL;
    private final RowColumnValueStore<TenantId, MiruReadTrackingWALRow, MiruReadTrackingSipWALColumnKey, Long, ? extends Exception>
        readTrackingSipWAL;

    @Inject
    MiruWALInitializer(
        RowColumnValueStore<TenantId, MiruActivityWALRow, MiruActivityWALColumnKey, MiruPartitionedActivity, ? extends Exception> activityWAL,
        RowColumnValueStore<TenantId, MiruActivityWALRow, MiruActivitySipWALColumnKey, MiruPartitionedActivity, ? extends Exception> activitySipWAL,
        RowColumnValueStore<TenantId, MiruReadTrackingWALRow, MiruReadTrackingWALColumnKey, MiruPartitionedActivity, ? extends Exception> readTrackingWAL,
        RowColumnValueStore<TenantId, MiruReadTrackingWALRow, MiruReadTrackingSipWALColumnKey, Long, ? extends Exception> readTrackingSipWAL) {
        this.activityWAL = Preconditions.checkNotNull(activityWAL);
        this.activitySipWAL = Preconditions.checkNotNull(activitySipWAL);
        this.readTrackingWAL = Preconditions.checkNotNull(readTrackingWAL);
        this.readTrackingSipWAL = Preconditions.checkNotNull(readTrackingSipWAL);
    }

    public RowColumnValueStore<TenantId, MiruActivityWALRow, MiruActivityWALColumnKey, MiruPartitionedActivity,
        ? extends Exception> getActivityWAL() {
        return activityWAL;
    }

    public RowColumnValueStore<TenantId, MiruActivityWALRow, MiruActivitySipWALColumnKey, MiruPartitionedActivity,
        ? extends Exception> getActivitySipWAL() {
        return activitySipWAL;
    }

    public RowColumnValueStore<TenantId, MiruReadTrackingWALRow, MiruReadTrackingWALColumnKey, MiruPartitionedActivity,
        ? extends Exception> getReadTrackingWAL() {
        return readTrackingWAL;
    }

    public RowColumnValueStore<TenantId, MiruReadTrackingWALRow, MiruReadTrackingSipWALColumnKey, Long,
        ? extends Exception> getReadTrackingSipWAL() {
        return readTrackingSipWAL;
    }
}
