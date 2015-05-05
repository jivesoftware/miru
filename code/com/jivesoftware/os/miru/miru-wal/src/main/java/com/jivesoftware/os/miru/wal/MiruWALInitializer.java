package com.jivesoftware.os.miru.wal;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.jivesoftware.os.miru.api.activity.MiruPartitionId;
import com.jivesoftware.os.miru.api.activity.MiruPartitionedActivity;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.api.marshall.JacksonJsonObjectTypeMarshaller;
import com.jivesoftware.os.miru.api.marshall.MiruPartitionIdMarshaller;
import com.jivesoftware.os.miru.api.marshall.MiruTenantIdMarshaller;
import com.jivesoftware.os.miru.api.marshall.MiruVoidByte;
import com.jivesoftware.os.miru.api.marshall.MiruVoidByteMarshaller;
import com.jivesoftware.os.miru.api.wal.MiruActivityLookupEntry;
import com.jivesoftware.os.miru.api.wal.MiruRangeLookupColumnKey;
import com.jivesoftware.os.miru.wal.activity.rcvs.MiruActivitySipWALColumnKey;
import com.jivesoftware.os.miru.wal.activity.rcvs.MiruActivitySipWALColumnKeyMarshaller;
import com.jivesoftware.os.miru.wal.activity.rcvs.MiruActivityWALColumnKey;
import com.jivesoftware.os.miru.wal.activity.rcvs.MiruActivityWALColumnKeyMarshaller;
import com.jivesoftware.os.miru.wal.activity.rcvs.MiruActivityWALRow;
import com.jivesoftware.os.miru.wal.activity.rcvs.MiruActivityWALRowMarshaller;
import com.jivesoftware.os.miru.wal.lookup.MiruActivityLookupEntryMarshaller;
import com.jivesoftware.os.miru.wal.lookup.MiruRangeLookupColumnKeyMarshaller;
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
import com.jivesoftware.os.rcvs.marshall.primatives.IntegerTypeMarshaller;
import com.jivesoftware.os.rcvs.marshall.primatives.LongTypeMarshaller;

public class MiruWALInitializer {

    public MiruWAL initialize(String tableNameSpace,
        RowColumnValueStoreInitializer<? extends Exception> rowColumnValueStoreInitializer,
        ObjectMapper objectMapper)
        throws Exception {

        // Miru ActivityWAL
        RowColumnValueStore<MiruTenantId, MiruActivityWALRow, MiruActivityWALColumnKey, MiruPartitionedActivity, ? extends Exception> activityWAL =
            rowColumnValueStoreInitializer.initialize(
                tableNameSpace,
                "miru.activity.wal", "a", new String[] { "s" }, new DefaultRowColumnValueStoreMarshaller<>(
                    new MiruTenantIdMarshaller(),
                    new SaltingDelegatingMarshaller<>(new MiruActivityWALRowMarshaller()),
                    new MiruActivityWALColumnKeyMarshaller(),
                    new JacksonJsonObjectTypeMarshaller<>(MiruPartitionedActivity.class, objectMapper)),
                new CurrentTimestamper()
            );

        // Miru ActivitySipWAL
        RowColumnValueStore<MiruTenantId, MiruActivityWALRow, MiruActivitySipWALColumnKey, MiruPartitionedActivity, ? extends Exception> activitySipWAL =
            rowColumnValueStoreInitializer.initialize(
                tableNameSpace,
                "miru.activity.wal", "s", new DefaultRowColumnValueStoreMarshaller<>(
                    new MiruTenantIdMarshaller(),
                    new SaltingDelegatingMarshaller<>(new MiruActivityWALRowMarshaller()),
                    new MiruActivitySipWALColumnKeyMarshaller(),
                    new JacksonJsonObjectTypeMarshaller<>(MiruPartitionedActivity.class, objectMapper)),
                new CurrentTimestamper()
            );

        // Miru ReadTrackingWAL
        RowColumnValueStore<MiruTenantId, MiruReadTrackingWALRow, MiruReadTrackingWALColumnKey, MiruPartitionedActivity, ? extends Exception> readTrackingWAL =
            rowColumnValueStoreInitializer.initialize(
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

        // Miru Activity Lookup Table
        RowColumnValueStore<MiruVoidByte, MiruTenantId, Long, MiruActivityLookupEntry, ? extends Exception> activityLookupTable =
            rowColumnValueStoreInitializer.initialize(
                tableNameSpace,
                "miru.lookup.t", // Tenant
                "a",
                new DefaultRowColumnValueStoreMarshaller<>(
                    new MiruVoidByteMarshaller(),
                    new SaltingDelegatingMarshaller<>(new MiruTenantIdMarshaller()),
                    new LongTypeMarshaller(),
                    new MiruActivityLookupEntryMarshaller()),
                new CurrentTimestamper()
            );

        // Miru Range Lookup Table
        RowColumnValueStore<MiruVoidByte, MiruTenantId, MiruRangeLookupColumnKey, Long, ? extends Exception> rangeLookupTable =
            rowColumnValueStoreInitializer.initialize(
                tableNameSpace,
                "miru.ranges.t", // Tenant
                "r",
                new DefaultRowColumnValueStoreMarshaller<>(
                    new MiruVoidByteMarshaller(),
                    new SaltingDelegatingMarshaller<>(new MiruTenantIdMarshaller()),
                    new MiruRangeLookupColumnKeyMarshaller(),
                    new LongTypeMarshaller()),
                new CurrentTimestamper()
            );

        // Miru Writer + PartitionId Registry
        RowColumnValueStore<MiruVoidByte, MiruTenantId, Integer, MiruPartitionId, ? extends Exception> writerPartitionRegistry =
            rowColumnValueStoreInitializer.initialize(
                tableNameSpace,
                "miru.reg.t", // Tenant
                "p",
                new String[] { "t", "c", "s" },
                new DefaultRowColumnValueStoreMarshaller<>(
                    new MiruVoidByteMarshaller(),
                    new SaltingDelegatingMarshaller<>(new MiruTenantIdMarshaller()),
                    new IntegerTypeMarshaller(),
                    new MiruPartitionIdMarshaller()),
                new CurrentTimestamper()
            );

        return new MiruWAL(activityWAL, activitySipWAL, readTrackingWAL, readTrackingSipWAL, activityLookupTable, rangeLookupTable, writerPartitionRegistry);
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
        private final RowColumnValueStore<MiruVoidByte,
            MiruTenantId, Long, MiruActivityLookupEntry, ? extends Exception> activityLookupTable;
        private final RowColumnValueStore<MiruVoidByte,
            MiruTenantId, MiruRangeLookupColumnKey, Long, ? extends Exception> rangeLookupTable;
        private final RowColumnValueStore<MiruVoidByte,
            MiruTenantId, Integer, MiruPartitionId, ? extends Exception> writerPartitionRegistry;

        public MiruWAL(
            RowColumnValueStore<MiruTenantId,
                MiruActivityWALRow, MiruActivityWALColumnKey, MiruPartitionedActivity, ? extends Exception> activityWAL,
            RowColumnValueStore<MiruTenantId,
                MiruActivityWALRow, MiruActivitySipWALColumnKey, MiruPartitionedActivity, ? extends Exception> activitySipWAL,
            RowColumnValueStore<MiruTenantId,
                MiruReadTrackingWALRow, MiruReadTrackingWALColumnKey, MiruPartitionedActivity, ? extends Exception> readTrackingWAL,
            RowColumnValueStore<MiruTenantId,
                MiruReadTrackingWALRow, MiruReadTrackingSipWALColumnKey, Long, ? extends Exception> readTrackingSipWAL,
            RowColumnValueStore<MiruVoidByte,
                MiruTenantId, Long, MiruActivityLookupEntry, ? extends Exception> activityLookupTable,
            RowColumnValueStore<MiruVoidByte,
                MiruTenantId, MiruRangeLookupColumnKey, Long, ? extends Exception> rangeLookupTable,
            RowColumnValueStore<MiruVoidByte,
                MiruTenantId, Integer, MiruPartitionId, ? extends Exception> writerPartitionRegistry) {
            this.activityWAL = activityWAL;
            this.activitySipWAL = activitySipWAL;
            this.readTrackingWAL = readTrackingWAL;
            this.readTrackingSipWAL = readTrackingSipWAL;
            this.activityLookupTable = activityLookupTable;
            this.rangeLookupTable = rangeLookupTable;
            this.writerPartitionRegistry = writerPartitionRegistry;
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

        public RowColumnValueStore<MiruVoidByte,
            MiruTenantId, Long, MiruActivityLookupEntry, ? extends Exception> getActivityLookupTable() {
            return activityLookupTable;
        }

        public RowColumnValueStore<MiruVoidByte,
            MiruTenantId, MiruRangeLookupColumnKey, Long, ? extends Exception> getRangeLookupTable() {
            return rangeLookupTable;
        }

        public RowColumnValueStore<MiruVoidByte,
            MiruTenantId, Integer, MiruPartitionId, ? extends Exception> getWriterPartitionRegistry() {
            return writerPartitionRegistry;
        }

    }
}
