package com.jivesoftware.os.miru.wal;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.jivesoftware.os.jive.utils.id.SaltingDelegatingMarshaller;
import com.jivesoftware.os.jive.utils.id.TenantId;
import com.jivesoftware.os.jive.utils.id.TenantIdMarshaller;
import com.jivesoftware.os.jive.utils.row.column.value.store.api.DefaultRowColumnValueStoreMarshaller;
import com.jivesoftware.os.jive.utils.row.column.value.store.api.RowColumnValueStore;
import com.jivesoftware.os.jive.utils.row.column.value.store.api.SetOfSortedMapsImplInitializer;
import com.jivesoftware.os.jive.utils.row.column.value.store.api.timestamper.CurrentTimestamper;
import com.jivesoftware.os.jive.utils.row.column.value.store.marshall.primatives.LongTypeMarshaller;
import com.jivesoftware.os.miru.api.activity.MiruPartitionedActivity;
import com.jivesoftware.os.miru.cluster.marshaller.JacksonJsonObjectTypeMarshaller;
import com.jivesoftware.os.miru.wal.activity.hbase.MiruActivitySipWALColumnKey;
import com.jivesoftware.os.miru.wal.activity.hbase.MiruActivitySipWALColumnKeyMarshaller;
import com.jivesoftware.os.miru.wal.activity.hbase.MiruActivityWALColumnKey;
import com.jivesoftware.os.miru.wal.activity.hbase.MiruActivityWALColumnKeyMarshaller;
import com.jivesoftware.os.miru.wal.activity.hbase.MiruActivityWALRow;
import com.jivesoftware.os.miru.wal.activity.hbase.MiruActivityWALRowMarshaller;
import com.jivesoftware.os.miru.wal.readtracking.hbase.MiruReadTrackingSipWALColumnKey;
import com.jivesoftware.os.miru.wal.readtracking.hbase.MiruReadTrackingSipWALColumnKeyMarshaller;
import com.jivesoftware.os.miru.wal.readtracking.hbase.MiruReadTrackingWALColumnKey;
import com.jivesoftware.os.miru.wal.readtracking.hbase.MiruReadTrackingWALColumnKeyMarshaller;
import com.jivesoftware.os.miru.wal.readtracking.hbase.MiruReadTrackingWALRow;
import com.jivesoftware.os.miru.wal.readtracking.hbase.MiruReadTrackingWALRowMarshaller;

public class MiruWALInitializer {

    public MiruWAL initialize(String tableNameSpace,
        SetOfSortedMapsImplInitializer<? extends Exception> setOfSortedMapsImplInitializer,
        ObjectMapper objectMapper) throws Exception {

        // Miru ActivityWAL
        RowColumnValueStore<TenantId, MiruActivityWALRow, MiruActivityWALColumnKey, MiruPartitionedActivity,
            ? extends Exception> activityWAL = setOfSortedMapsImplInitializer.initialize(
            tableNameSpace,
            "miru.activity.wal", "a", new String[] { "s" }, new DefaultRowColumnValueStoreMarshaller<>(
                new TenantIdMarshaller(),
                new SaltingDelegatingMarshaller<>(new MiruActivityWALRowMarshaller()),
                new MiruActivityWALColumnKeyMarshaller(),
                new JacksonJsonObjectTypeMarshaller<>(MiruPartitionedActivity.class, objectMapper)), new CurrentTimestamper()
        );

        // Miru ActivitySipWAL
        RowColumnValueStore<TenantId, MiruActivityWALRow, MiruActivitySipWALColumnKey, MiruPartitionedActivity,
            ? extends Exception> activitySipWAL = setOfSortedMapsImplInitializer.initialize(
            tableNameSpace,
            "miru.activity.wal", "s", new DefaultRowColumnValueStoreMarshaller<>(
                new TenantIdMarshaller(),
                new SaltingDelegatingMarshaller<>(new MiruActivityWALRowMarshaller()),
                new MiruActivitySipWALColumnKeyMarshaller(),
                new JacksonJsonObjectTypeMarshaller<>(MiruPartitionedActivity.class, objectMapper)), new CurrentTimestamper()
        );

        // Miru ReadTrackingWAL
        RowColumnValueStore<TenantId, MiruReadTrackingWALRow, MiruReadTrackingWALColumnKey, MiruPartitionedActivity,
            ? extends Exception> readTrackingWAL = setOfSortedMapsImplInitializer.initialize(
            tableNameSpace,
            "miru.readtracking.wal", "r", new String[] { "s" }, new DefaultRowColumnValueStoreMarshaller<>(
                new TenantIdMarshaller(),
                new SaltingDelegatingMarshaller<>(new MiruReadTrackingWALRowMarshaller()),
                new MiruReadTrackingWALColumnKeyMarshaller(),
                new JacksonJsonObjectTypeMarshaller<>(MiruPartitionedActivity.class, objectMapper)), new CurrentTimestamper()
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

        return new MiruWAL(activityWAL, activitySipWAL, readTrackingWAL, readTrackingSipWAL);
    }

    static public class MiruWAL {

        private final RowColumnValueStore<TenantId, MiruActivityWALRow, MiruActivityWALColumnKey, MiruPartitionedActivity, ? extends Exception> activityWAL;
        private final RowColumnValueStore<TenantId, MiruActivityWALRow, MiruActivitySipWALColumnKey, MiruPartitionedActivity,
            ? extends Exception> activitySipWAL;
        private final RowColumnValueStore<TenantId, MiruReadTrackingWALRow, MiruReadTrackingWALColumnKey, MiruPartitionedActivity,
            ? extends Exception> readTrackingWAL;
        private final RowColumnValueStore<TenantId, MiruReadTrackingWALRow, MiruReadTrackingSipWALColumnKey, Long, ? extends Exception> readTrackingSipWAL;

        public MiruWAL(RowColumnValueStore<TenantId, MiruActivityWALRow, MiruActivityWALColumnKey, MiruPartitionedActivity, ? extends Exception> activityWAL,
            RowColumnValueStore<TenantId, MiruActivityWALRow, MiruActivitySipWALColumnKey, MiruPartitionedActivity, ? extends Exception> activitySipWAL,
            RowColumnValueStore<TenantId, MiruReadTrackingWALRow, MiruReadTrackingWALColumnKey, MiruPartitionedActivity, ? extends Exception> readTrackingWAL,
            RowColumnValueStore<TenantId, MiruReadTrackingWALRow, MiruReadTrackingSipWALColumnKey, Long, ? extends Exception> readTrackingSipWAL) {
            this.activityWAL = activityWAL;
            this.activitySipWAL = activitySipWAL;
            this.readTrackingWAL = readTrackingWAL;
            this.readTrackingSipWAL = readTrackingSipWAL;
        }

        public RowColumnValueStore<TenantId, MiruActivityWALRow, MiruActivityWALColumnKey, MiruPartitionedActivity, ? extends Exception> getActivityWAL() {
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

        public RowColumnValueStore<TenantId, MiruReadTrackingWALRow, MiruReadTrackingSipWALColumnKey, Long, ? extends Exception> getReadTrackingSipWAL() {
            return readTrackingSipWAL;
        }
    }
}
