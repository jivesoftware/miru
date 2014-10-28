package com.jivesoftware.os.miru.service.stream;

import com.google.common.collect.Interners;
import com.google.common.util.concurrent.MoreExecutors;
import com.googlecode.javaewah.EWAHCompressedBitmap;
import com.jivesoftware.os.filer.io.FilerIO;
import com.jivesoftware.os.filer.io.HeapByteBufferFactory;
import com.jivesoftware.os.miru.api.MiruBackingStorage;
import com.jivesoftware.os.miru.api.MiruHost;
import com.jivesoftware.os.miru.api.MiruPartitionCoord;
import com.jivesoftware.os.miru.api.activity.MiruActivity;
import com.jivesoftware.os.miru.api.activity.MiruPartitionId;
import com.jivesoftware.os.miru.api.activity.MiruPartitionedActivity;
import com.jivesoftware.os.miru.api.activity.schema.DefaultMiruSchemaDefinition;
import com.jivesoftware.os.miru.api.activity.schema.MiruSchema;
import com.jivesoftware.os.miru.api.base.MiruIBA;
import com.jivesoftware.os.miru.api.base.MiruStreamId;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.api.base.MiruTermId;
import com.jivesoftware.os.miru.api.field.MiruFieldName;
import com.jivesoftware.os.miru.api.query.filter.MiruAuthzExpression;
import com.jivesoftware.os.miru.plugin.bitmap.MiruBitmaps;
import com.jivesoftware.os.miru.plugin.index.MiruActivityAndId;
import com.jivesoftware.os.miru.plugin.index.MiruActivityInternExtern;
import com.jivesoftware.os.miru.plugin.schema.SingleSchemaProvider;
import com.jivesoftware.os.miru.service.MiruServiceConfig;
import com.jivesoftware.os.miru.service.bitmap.MiruBitmapsEWAH;
import com.jivesoftware.os.miru.service.locator.MiruTempDirectoryResourceLocator;
import com.jivesoftware.os.miru.wal.readtracking.MiruReadTrackingWALReaderImpl;
import com.jivesoftware.os.miru.wal.readtracking.hbase.MiruReadTrackingSipWALColumnKey;
import com.jivesoftware.os.miru.wal.readtracking.hbase.MiruReadTrackingWALColumnKey;
import com.jivesoftware.os.miru.wal.readtracking.hbase.MiruReadTrackingWALRow;
import com.jivesoftware.os.rcvs.inmemory.RowColumnValueStoreImpl;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.concurrent.Executors;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;

/**
 *
 */
public class MiruContextFactoryTest {

    private MiruSchema schema;
    private MiruContextFactory streamFactory;
    private MiruHost host = new MiruHost("localhost", 49_600);
    private MiruBitmaps<EWAHCompressedBitmap> bitmaps;

    @BeforeMethod
    public void setUp() throws Exception {
        MiruServiceConfig config = mock(MiruServiceConfig.class);
        when(config.getBitsetBufferSize()).thenReturn(32);
        when(config.getDefaultStorage()).thenReturn(MiruBackingStorage.memory.name());

        RowColumnValueStoreImpl<MiruTenantId, MiruReadTrackingWALRow, MiruReadTrackingWALColumnKey, MiruPartitionedActivity> readTrackingWAL =
            new RowColumnValueStoreImpl<>();
        RowColumnValueStoreImpl<MiruTenantId, MiruReadTrackingWALRow, MiruReadTrackingSipWALColumnKey, Long> readTrackingSipWAL =
            new RowColumnValueStoreImpl<>();

        schema = new MiruSchema(DefaultMiruSchemaDefinition.FIELDS);
        bitmaps = new MiruBitmapsEWAH(4);
        MiruActivityInternExtern activityInterner = new MiruActivityInternExtern(Interners.<MiruIBA>newWeakInterner(), Interners.<MiruTermId>newWeakInterner(),
            Interners.<MiruTenantId>newWeakInterner(), Interners.<String>newWeakInterner());

        streamFactory = new MiruContextFactory(new SingleSchemaProvider(schema),
            Executors.newSingleThreadExecutor(),
            new MiruReadTrackingWALReaderImpl(readTrackingWAL, readTrackingSipWAL),
            new MiruTempDirectoryResourceLocator(),
            new MiruTempDirectoryResourceLocator(),
            20,
            100,
            false,
            MiruBackingStorage.memory,
            activityInterner);

    }

    @Test (enabled = true, description = "This test is disk dependent, disable if it flaps or becomes slow")
    public void testCopyToDisk() throws Exception {
        int numberOfActivities = 5;

        MiruTenantId tenantId = new MiruTenantId("ace5".getBytes());
        MiruPartitionId partitionId = MiruPartitionId.of(0);
        MiruPartitionCoord coord = new MiruPartitionCoord(tenantId, partitionId, host);

        MiruContext<EWAHCompressedBitmap> inMemoryStream = streamFactory.allocate(bitmaps, coord, MiruBackingStorage.memory, new HeapByteBufferFactory());

        for (int i = 0; i < numberOfActivities; i++) {
            String[] authz = { "aaaabbbbcccc" };
            MiruActivity activity = new MiruActivity.Builder(tenantId, (long) i, authz, 0)
                .putFieldValue(MiruFieldName.OBJECT_ID.getFieldName(), String.valueOf(i))
                .build();
            int id = inMemoryStream.getTimeIndex().nextId((long) i);
            inMemoryStream.getIndexContext().index(new ArrayList<>(Arrays.asList(new MiruActivityAndId<>(activity, id))), MoreExecutors.sameThreadExecutor());
            inMemoryStream.getIndexContext().remove(activity, id);
        }

        MiruContext<EWAHCompressedBitmap> onDiskStream = streamFactory.copyToDisk(bitmaps, coord, inMemoryStream);

        assertEquals(onDiskStream.getRequestContext().timeIndex.getSmallestTimestamp(), inMemoryStream.getRequestContext().timeIndex.getSmallestTimestamp());
        assertEquals(onDiskStream.getRequestContext().timeIndex.getLargestTimestamp(), inMemoryStream.getRequestContext().timeIndex.getLargestTimestamp());

        MiruAuthzExpression authzExpression = new MiruAuthzExpression(Arrays.asList("aaaabbbbcccc"));
        assertEquals(onDiskStream.getRequestContext().authzIndex.getCompositeAuthz(authzExpression),
            inMemoryStream.getRequestContext().authzIndex.getCompositeAuthz(authzExpression));
        assertEquals(onDiskStream.getRequestContext().removalIndex.getIndex(),
            inMemoryStream.getRequestContext().removalIndex.getIndex());

        for (int i = 0; i < numberOfActivities; i++) {
            assertEquals(onDiskStream.getRequestContext().timeIndex.getTimestamp(i), inMemoryStream.getRequestContext().timeIndex.getTimestamp(i));
            assertEquals(onDiskStream.getRequestContext().activityIndex.get(tenantId, i), inMemoryStream.getRequestContext().activityIndex.get(tenantId, i));

            int fieldId = schema.getFieldId(MiruFieldName.OBJECT_ID.getFieldName());
            if (fieldId >= 0) {
                MiruTermId termId = new MiruTermId(String.valueOf(i).getBytes());
                assertEquals(onDiskStream.getRequestContext().fieldIndex.getField(fieldId).getInvertedIndex(termId).get().getIndex(),
                    inMemoryStream.getRequestContext().fieldIndex.getField(fieldId).getInvertedIndex(termId).get().getIndex());
            }
        }
    }

    @Test
    public void testFindBackingStorage_disk() throws Exception {
        MiruTenantId tenantId = new MiruTenantId("findBackingStorage_disk".getBytes());
        MiruPartitionId partitionId = MiruPartitionId.of(0);
        MiruPartitionCoord coord = new MiruPartitionCoord(tenantId, partitionId, host);

        MiruContext<EWAHCompressedBitmap> inMem = minimalInMemory(coord);

        MiruContext<EWAHCompressedBitmap> miruContext = streamFactory.copyToDisk(bitmaps, coord, inMem);
        streamFactory.markStorage(coord, MiruBackingStorage.disk);
        streamFactory.close(miruContext);

        assertEquals(streamFactory.findBackingStorage(coord), MiruBackingStorage.disk);
    }

    @Test
    public void testFindBackingStorage_memMap() throws Exception {
        MiruTenantId tenantId = new MiruTenantId("findBackingStorage_memMap".getBytes());
        MiruPartitionId partitionId = MiruPartitionId.of(0);
        MiruPartitionCoord coord = new MiruPartitionCoord(tenantId, partitionId, host);

        MiruContext<EWAHCompressedBitmap> inMem = minimalInMemory(coord);

        MiruContext<EWAHCompressedBitmap> miruContext = streamFactory.copyMemMapped(bitmaps, coord, inMem);
        streamFactory.markStorage(coord, MiruBackingStorage.mem_mapped);
        streamFactory.close(miruContext);

        assertEquals(streamFactory.findBackingStorage(coord), MiruBackingStorage.mem_mapped);
    }

    private MiruContext<EWAHCompressedBitmap> minimalInMemory(MiruPartitionCoord coord) throws Exception {
        //TODO detecting backing storage fails if we haven't indexed at least 1 term for every field, 1 inbox, 1 unread
        MiruActivity.Builder builder = new MiruActivity.Builder(coord.tenantId, 0, new String[]{ "abcd" }, 0);
        for (MiruFieldName fieldName : MiruFieldName.values()) {
            builder.putFieldValue(fieldName.getFieldName(), "defg");
        }

        MiruContext<EWAHCompressedBitmap> inMem = streamFactory.allocate(bitmaps, coord, MiruBackingStorage.memory, new HeapByteBufferFactory());
        int id = inMem.getTimeIndex().nextId(System.currentTimeMillis());
        MiruStreamId streamId = new MiruStreamId(FilerIO.longBytes(0));
        inMem.getIndexContext().index(new ArrayList<>(Arrays.asList(new MiruActivityAndId<>(builder.build(), id))), MoreExecutors.sameThreadExecutor());
        inMem.getRequestContext().inboxIndex.index(streamId, id);
        inMem.getRequestContext().unreadTrackingIndex.index(streamId, id);
        return inMem;
    }
}
