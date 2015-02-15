package com.jivesoftware.os.miru.wal;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.jivesoftware.os.miru.api.activity.MiruPartitionedActivity;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.cluster.marshaller.JacksonJsonObjectTypeMarshaller;
import com.jivesoftware.os.miru.cluster.marshaller.MiruTenantIdMarshaller;
import com.jivesoftware.os.miru.wal.activity.rcvs.MiruActivitySipWALColumnKey;
import com.jivesoftware.os.miru.wal.activity.rcvs.MiruActivitySipWALColumnKeyMarshaller;
import com.jivesoftware.os.miru.wal.activity.rcvs.MiruActivityWALColumnKey;
import com.jivesoftware.os.miru.wal.activity.rcvs.MiruActivityWALColumnKeyMarshaller;
import com.jivesoftware.os.miru.wal.activity.rcvs.MiruActivityWALRow;
import com.jivesoftware.os.miru.wal.activity.rcvs.MiruActivityWALRowMarshaller;
import com.jivesoftware.os.miru.wal.readtracking.rcvs.MiruReadTrackingSipWALColumnKey;
import com.jivesoftware.os.miru.wal.readtracking.rcvs.MiruReadTrackingSipWALColumnKeyMarshaller;
import com.jivesoftware.os.miru.wal.readtracking.rcvs.MiruReadTrackingWALColumnKey;
import com.jivesoftware.os.miru.wal.readtracking.rcvs.MiruReadTrackingWALColumnKeyMarshaller;
import com.jivesoftware.os.miru.wal.readtracking.rcvs.MiruReadTrackingWALRow;
import com.jivesoftware.os.miru.wal.readtracking.rcvs.MiruReadTrackingWALRowMarshaller;
import com.jivesoftware.os.rcvs.api.DefaultRowColumnValueStoreMarshaller;
import com.jivesoftware.os.rcvs.api.RowColumnValueStore;
import com.jivesoftware.os.rcvs.api.RowColumnValueStoreInitializer;
import com.jivesoftware.os.rcvs.api.timestamper.CurrentTimestamper;
import com.jivesoftware.os.rcvs.marshall.id.SaltingDelegatingMarshaller;
import com.jivesoftware.os.rcvs.marshall.primatives.LongTypeMarshaller;

public class MiruWALInitializer {

    public MiruWAL initialize(String tableNameSpace,
        RowColumnValueStoreInitializer<? extends Exception> rowColumnValueStoreInitializer,
        ObjectMapper objectMapper)
        throws Exception {

        // Miru ActivityWAL
        RowColumnValueStore<MiruTenantId, MiruActivityWALRow, MiruActivityWALColumnKey, MiruPartitionedActivity,
            ? extends Exception> activityWAL = rowColumnValueStoreInitializer.initialize(
            tableNameSpace,
            "miru.activity.wal", "a", new String[] { "s" }, new DefaultRowColumnValueStoreMarshaller<>(
                new MiruTenantIdMarshaller(),
                new SaltingDelegatingMarshaller<>(new MiruActivityWALRowMarshaller()),
                new MiruActivityWALColumnKeyMarshaller(),
                new JacksonJsonObjectTypeMarshaller<>(MiruPartitionedActivity.class, objectMapper)),
            new CurrentTimestamper()
        );

        // Miru ActivitySipWAL
        RowColumnValueStore<MiruTenantId, MiruActivityWALRow, MiruActivitySipWALColumnKey, MiruPartitionedActivity,
            ? extends Exception> activitySipWAL = rowColumnValueStoreInitializer.initialize(
            tableNameSpace,
            "miru.activity.wal", "s", new DefaultRowColumnValueStoreMarshaller<>(
                new MiruTenantIdMarshaller(),
                new SaltingDelegatingMarshaller<>(new MiruActivityWALRowMarshaller()),
                new MiruActivitySipWALColumnKeyMarshaller(),
                new JacksonJsonObjectTypeMarshaller<>(MiruPartitionedActivity.class, objectMapper)),
            new CurrentTimestamper()
        );

        // Miru ReadTrackingWAL
        RowColumnValueStore<MiruTenantId, MiruReadTrackingWALRow, MiruReadTrackingWALColumnKey, MiruPartitionedActivity,
            ? extends Exception> readTrackingWAL = rowColumnValueStoreInitializer.initialize(
            tableNameSpace,
            "miru.readtracking.wal", "r", new String[] { "s" }, new DefaultRowColumnValueStoreMarshaller<>(
                new MiruTenantIdMarshaller(),
                new SaltingDelegatingMarshaller<>(new MiruReadTrackingWALRowMarshaller()),
                new MiruReadTrackingWALColumnKeyMarshaller(),
                new JacksonJsonObjectTypeMarshaller<>(MiruPartitionedActivity.class, objectMapper)),
            new CurrentTimestamper()
        );

        // Miru ReadTrackingSipWAL
        RowColumnValueStore<MiruTenantId, MiruReadTrackingWALRow, MiruReadTrackingSipWALColumnKey, Long, ? extends Exception> readTrackingSipWAL =
            rowColumnValueStoreInitializer.initialize(
                tableNameSpace,
                "miru.readtracking.wal", "s", new DefaultRowColumnValueStoreMarshaller<>(
                    new MiruTenantIdMarshaller(),
                    new SaltingDelegatingMarshaller<>(new MiruReadTrackingWALRowMarshaller()),
                    new MiruReadTrackingSipWALColumnKeyMarshaller(),
                    new LongTypeMarshaller()),
                new CurrentTimestamper()
            );

        return new MiruWAL(activityWAL, activitySipWAL, readTrackingWAL, readTrackingSipWAL);
    }

    static public class MiruWAL {

        private final RowColumnValueStore<MiruTenantId,
            MiruActivityWALRow, MiruActivityWALColumnKey, MiruPartitionedActivity, ? extends Exception> activityWAL;
        private final RowColumnValueStore<MiruTenantId,
            MiruActivityWALRow, MiruActivitySipWALColumnKey, MiruPartitionedActivity, ? extends Exception> activitySipWAL;
        private final RowColumnValueStore<MiruTenantId,
            MiruReadTrackingWALRow, MiruReadTrackingWALColumnKey, MiruPartitionedActivity, ? extends Exception> readTrackingWAL;
        private final RowColumnValueStore<MiruTenantId,
            MiruReadTrackingWALRow, MiruReadTrackingSipWALColumnKey, Long, ? extends Exception> readTrackingSipWAL;

        public MiruWAL(
            RowColumnValueStore<MiruTenantId,
                MiruActivityWALRow, MiruActivityWALColumnKey, MiruPartitionedActivity, ? extends Exception> activityWAL,
            RowColumnValueStore<MiruTenantId,
                MiruActivityWALRow, MiruActivitySipWALColumnKey, MiruPartitionedActivity, ? extends Exception> activitySipWAL,
            RowColumnValueStore<MiruTenantId,
                MiruReadTrackingWALRow, MiruReadTrackingWALColumnKey, MiruPartitionedActivity, ? extends Exception> readTrackingWAL,
            RowColumnValueStore<MiruTenantId,
                MiruReadTrackingWALRow, MiruReadTrackingSipWALColumnKey, Long, ? extends Exception> readTrackingSipWAL) {
            this.activityWAL = activityWAL;
            this.activitySipWAL = activitySipWAL;
            this.readTrackingWAL = readTrackingWAL;
            this.readTrackingSipWAL = readTrackingSipWAL;
        }

        public RowColumnValueStore<MiruTenantId,
            MiruActivityWALRow, MiruActivityWALColumnKey, MiruPartitionedActivity, ? extends Exception> getActivityWAL() {
            return activityWAL;
        }

        public RowColumnValueStore<MiruTenantId,
            MiruActivityWALRow, MiruActivitySipWALColumnKey, MiruPartitionedActivity, ? extends Exception> getActivitySipWAL() {
            return activitySipWAL;
        }

        public RowColumnValueStore<MiruTenantId,
            MiruReadTrackingWALRow, MiruReadTrackingWALColumnKey, MiruPartitionedActivity, ? extends Exception> getReadTrackingWAL() {
            return readTrackingWAL;
        }

        public RowColumnValueStore<MiruTenantId,
            MiruReadTrackingWALRow, MiruReadTrackingSipWALColumnKey, Long, ? extends Exception> getReadTrackingSipWAL() {
            return readTrackingSipWAL;
        }
    }
}
