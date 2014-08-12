package com.jivesoftware.os.miru.service.stream;

import com.jivesoftware.os.miru.api.MiruBackingStorage;
import com.jivesoftware.os.miru.api.MiruHost;
import com.jivesoftware.os.miru.api.MiruPartitionCoord;
import com.jivesoftware.os.miru.api.activity.MiruActivity;
import com.jivesoftware.os.miru.api.activity.MiruPartitionId;
import com.jivesoftware.os.miru.api.activity.MiruPartitionedActivity;
import com.jivesoftware.os.miru.api.base.MiruStreamId;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.api.base.MiruTermId;
import com.jivesoftware.os.miru.api.field.MiruFieldName;
import com.jivesoftware.os.miru.api.query.filter.MiruAuthzExpression;
import com.jivesoftware.os.miru.service.MiruServiceConfig;
import com.jivesoftware.os.miru.service.schema.DefaultMiruSchemaDefinition;
import com.jivesoftware.os.miru.service.schema.MiruSchema;
import com.jivesoftware.os.miru.service.stream.locator.MiruTempDirectoryResourceLocator;
import com.jivesoftware.os.miru.wal.readtracking.MiruReadTrackingWALReaderImpl;
import com.jivesoftware.os.miru.wal.readtracking.hbase.MiruReadTrackingSipWALColumnKey;
import com.jivesoftware.os.miru.wal.readtracking.hbase.MiruReadTrackingWALColumnKey;
import com.jivesoftware.os.miru.wal.readtracking.hbase.MiruReadTrackingWALRow;
import com.jivesoftware.os.jive.utils.id.TenantId;
import com.jivesoftware.os.jive.utils.io.FilerIO;
import com.jivesoftware.os.jive.utils.row.column.value.store.inmemory.RowColumnValueStoreImpl;
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
public class MiruStreamFactoryTest {

    private MiruSchema schema;
    private MiruStreamFactory streamFactory;
    private MiruHost host = new MiruHost("localhost", 49600);

    @BeforeMethod
    public void setUp() throws Exception {
        MiruServiceConfig config = mock(MiruServiceConfig.class);
        when(config.getBitsetBufferSize()).thenReturn(32);
        when(config.getDefaultStorage()).thenReturn(MiruBackingStorage.memory.name());

        RowColumnValueStoreImpl<TenantId, MiruReadTrackingWALRow, MiruReadTrackingWALColumnKey, MiruPartitionedActivity> readTrackingWAL =
            new RowColumnValueStoreImpl<>();
        RowColumnValueStoreImpl<TenantId, MiruReadTrackingWALRow, MiruReadTrackingSipWALColumnKey, Long> readTrackingSipWAL =
            new RowColumnValueStoreImpl<>();

        schema = new MiruSchema(DefaultMiruSchemaDefinition.SCHEMA);
        streamFactory = new MiruStreamFactory(
            config,
            schema,
            Executors.newSingleThreadExecutor(),
            new MiruReadTrackingWALReaderImpl(readTrackingWAL, readTrackingSipWAL),
            new MiruTempDirectoryResourceLocator(),
            new MiruTempDirectoryResourceLocator());

    }

    @Test(enabled = true, description = "This test is disk dependent, disable if it flaps or becomes slow")
    public void testCopyToDisk() throws Exception {
        int numberOfActivities = 5;

        MiruTenantId tenantId = new MiruTenantId("ace5".getBytes());
        MiruPartitionId partitionId = MiruPartitionId.of(0);
        MiruPartitionCoord coord = new MiruPartitionCoord(tenantId, partitionId, host);

        MiruStream inMemoryStream = streamFactory.allocate(coord, MiruBackingStorage.memory);

        for (int i = 0; i < numberOfActivities; i++) {
            String[] authz = { "aaaabbbbcccc" };
            MiruActivity activity = new MiruActivity.Builder(tenantId, (long) i, authz, 0)
                .putFieldValue(MiruFieldName.OBJECT_ID.getFieldName(), String.valueOf(i))
                .build();
            int id = inMemoryStream.getTimeIndex().nextId((long) i);
            inMemoryStream.getIndexStream().index(activity, id);
            inMemoryStream.getIndexStream().remove(activity, id);
        }

        MiruStream onDiskStream = streamFactory.copyToDisk(coord, inMemoryStream);

        assertEquals(onDiskStream.getQueryStream().timeIndex.getSmallestTimestamp(), inMemoryStream.getQueryStream().timeIndex.getSmallestTimestamp());
        assertEquals(onDiskStream.getQueryStream().timeIndex.getLargestTimestamp(), inMemoryStream.getQueryStream().timeIndex.getLargestTimestamp());

        MiruAuthzExpression authzExpression = new MiruAuthzExpression(Arrays.asList("aaaabbbbcccc"));
        assertEquals(onDiskStream.getQueryStream().authzIndex.getCompositeAuthz(authzExpression),
            inMemoryStream.getQueryStream().authzIndex.getCompositeAuthz(authzExpression));
        assertEquals(onDiskStream.getQueryStream().removalIndex.getIndex(),
            inMemoryStream.getQueryStream().removalIndex.getIndex());

        for (int i = 0; i < numberOfActivities; i++) {
            assertEquals(onDiskStream.getQueryStream().timeIndex.getTimestamp(i), inMemoryStream.getQueryStream().timeIndex.getTimestamp(i));
            assertEquals(onDiskStream.getQueryStream().activityIndex.get(i), inMemoryStream.getQueryStream().activityIndex.get(i));

            int fieldId = schema.getFieldId(MiruFieldName.OBJECT_ID.getFieldName());
            if (fieldId >= 0) {
                MiruTermId termId = new MiruTermId(String.valueOf(i).getBytes());
                assertEquals(onDiskStream.getQueryStream().fieldIndex.getField(fieldId).getInvertedIndex(termId).get().getIndex(),
                    inMemoryStream.getQueryStream().fieldIndex.getField(fieldId).getInvertedIndex(termId).get().getIndex());
            }
        }
    }

    @Test
    public void testFindBackingStorage_disk() throws Exception {
        MiruTenantId tenantId = new MiruTenantId("findBackingStorage_disk".getBytes());
        MiruPartitionId partitionId = MiruPartitionId.of(0);
        MiruPartitionCoord coord = new MiruPartitionCoord(tenantId, partitionId, host);

        MiruStream inMem = minimalInMemory(coord);

        MiruStream miruStream = streamFactory.copyToDisk(coord, inMem);
        streamFactory.markStorage(coord, MiruBackingStorage.disk);
        streamFactory.close(miruStream);

        assertEquals(streamFactory.findBackingStorage(coord), MiruBackingStorage.disk);
    }

    @Test
    public void testFindBackingStorage_memMap() throws Exception {
        MiruTenantId tenantId = new MiruTenantId("findBackingStorage_memMap".getBytes());
        MiruPartitionId partitionId = MiruPartitionId.of(0);
        MiruPartitionCoord coord = new MiruPartitionCoord(tenantId, partitionId, host);

        MiruStream inMem = minimalInMemory(coord);

        MiruStream miruStream = streamFactory.copyMemMapped(coord, inMem);
        streamFactory.markStorage(coord, MiruBackingStorage.mem_mapped);
        streamFactory.close(miruStream);

        assertEquals(streamFactory.findBackingStorage(coord), MiruBackingStorage.mem_mapped);
    }

    private MiruStream minimalInMemory(MiruPartitionCoord coord) throws Exception {
        //TODO detecting backing storage fails if we haven't indexed at least 1 term for every field, 1 inbox, 1 unread
        MiruActivity.Builder builder = new MiruActivity.Builder(coord.tenantId, 0, new String[] { "abcd" }, 0);
        for (MiruFieldName fieldName : MiruFieldName.values()) {
            builder.putFieldValue(fieldName.getFieldName(), "defg");
        }

        MiruStream inMem = streamFactory.allocate(coord, MiruBackingStorage.memory);
        int id = inMem.getTimeIndex().nextId(System.currentTimeMillis());
        MiruStreamId streamId = new MiruStreamId(FilerIO.longBytes(0));
        inMem.getIndexStream().index(builder.build(), id);
        inMem.getQueryStream().inboxIndex.index(streamId, id);
        inMem.getQueryStream().unreadTrackingIndex.index(streamId, id);
        return inMem;
    }
}
