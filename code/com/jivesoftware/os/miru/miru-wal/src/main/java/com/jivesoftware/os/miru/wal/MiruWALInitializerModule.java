package com.jivesoftware.os.miru.wal;

import com.google.common.base.Preconditions;
import com.google.inject.AbstractModule;
import com.google.inject.TypeLiteral;
import com.jivesoftware.os.jive.utils.id.TenantId;
import com.jivesoftware.os.jive.utils.row.column.value.store.api.RowColumnValueStore;
import com.jivesoftware.os.miru.api.activity.MiruPartitionedActivity;
import com.jivesoftware.os.miru.wal.activity.MiruActivityWALReader;
import com.jivesoftware.os.miru.wal.activity.MiruActivityWALWriter;
import com.jivesoftware.os.miru.wal.activity.hbase.MiruActivitySipWALColumnKey;
import com.jivesoftware.os.miru.wal.activity.hbase.MiruActivityWALColumnKey;
import com.jivesoftware.os.miru.wal.activity.hbase.MiruActivityWALRow;
import com.jivesoftware.os.miru.wal.readtracking.MiruReadTrackingWALReader;
import com.jivesoftware.os.miru.wal.readtracking.MiruReadTrackingWALWriter;
import com.jivesoftware.os.miru.wal.readtracking.hbase.MiruReadTrackingSipWALColumnKey;
import com.jivesoftware.os.miru.wal.readtracking.hbase.MiruReadTrackingWALColumnKey;
import com.jivesoftware.os.miru.wal.readtracking.hbase.MiruReadTrackingWALRow;

public class MiruWALInitializerModule extends AbstractModule {

    private final RowColumnValueStore<TenantId, MiruActivityWALRow, MiruActivityWALColumnKey, MiruPartitionedActivity, ? extends Exception>
        activityWAL;
    private final RowColumnValueStore<TenantId, MiruActivityWALRow, MiruActivitySipWALColumnKey, MiruPartitionedActivity, ? extends Exception>
        activitySipWAL;
    private final RowColumnValueStore<TenantId, MiruReadTrackingWALRow, MiruReadTrackingWALColumnKey, MiruPartitionedActivity, ? extends Exception>
        readTrackingWAL;
    private final RowColumnValueStore<TenantId, MiruReadTrackingWALRow, MiruReadTrackingSipWALColumnKey, Long, ? extends Exception>
        readTrackingSipWAL;

    private final Class<? extends MiruActivityWALReader> activityWALReaderClass;
    private final Class<? extends MiruActivityWALWriter> activityWALWriterClass;
    private final Class<? extends MiruReadTrackingWALReader> readTrackingWALReaderClass;
    private final Class<? extends MiruReadTrackingWALWriter> readTrackingWALWriterClass;

    public MiruWALInitializerModule(
        RowColumnValueStore<TenantId, MiruActivityWALRow, MiruActivityWALColumnKey, MiruPartitionedActivity, ? extends Exception> activityWAL,
        RowColumnValueStore<TenantId, MiruActivityWALRow, MiruActivitySipWALColumnKey, MiruPartitionedActivity, ? extends Exception> activitySipWAL,
        RowColumnValueStore<TenantId, MiruReadTrackingWALRow, MiruReadTrackingWALColumnKey, MiruPartitionedActivity, ? extends Exception> readTrackingWAL,
        RowColumnValueStore<TenantId, MiruReadTrackingWALRow, MiruReadTrackingSipWALColumnKey, Long, ? extends Exception> readTrackingSipWAL,
        Class<? extends MiruActivityWALReader> activityWALReaderClass,
        Class<? extends MiruActivityWALWriter> activityWALWriterClass,
        Class<? extends MiruReadTrackingWALReader> readTrackingWALReaderClass,
        Class<? extends MiruReadTrackingWALWriter> readTrackingWALWriterClass) {
        this.activityWAL = activityWAL;
        this.activitySipWAL = activitySipWAL;
        this.readTrackingWAL = readTrackingWAL;
        this.readTrackingSipWAL = readTrackingSipWAL;
        this.activityWALReaderClass = Preconditions.checkNotNull(activityWALReaderClass);
        this.activityWALWriterClass = Preconditions.checkNotNull(activityWALWriterClass);
        this.readTrackingWALReaderClass = Preconditions.checkNotNull(readTrackingWALReaderClass);
        this.readTrackingWALWriterClass = Preconditions.checkNotNull(readTrackingWALWriterClass);
    }

    @Override
    protected void configure() {
        bind(new TypeLiteral<RowColumnValueStore<TenantId, MiruActivityWALRow, MiruActivityWALColumnKey, MiruPartitionedActivity,
            ? extends Exception>>() { }).toInstance(activityWAL);
        bind(new TypeLiteral<RowColumnValueStore<TenantId, MiruActivityWALRow, MiruActivitySipWALColumnKey, MiruPartitionedActivity,
            ? extends Exception>>() { }).toInstance(activitySipWAL);
        bind(new TypeLiteral<RowColumnValueStore<TenantId, MiruReadTrackingWALRow, MiruReadTrackingWALColumnKey, MiruPartitionedActivity,
            ? extends Exception>>() { }).toInstance(readTrackingWAL);
        bind(new TypeLiteral<RowColumnValueStore<TenantId, MiruReadTrackingWALRow, MiruReadTrackingSipWALColumnKey, Long,
            ? extends Exception>>() { }).toInstance(readTrackingSipWAL);

        bind(MiruActivityWALReader.class).to(activityWALReaderClass);
        bind(MiruActivityWALWriter.class).to(activityWALWriterClass);
        bind(MiruReadTrackingWALReader.class).to(readTrackingWALReaderClass);
        bind(MiruReadTrackingWALWriter.class).to(readTrackingWALWriterClass);
    }

}
