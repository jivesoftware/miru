package com.jivesoftware.os.miru.service.stream;

import com.google.common.collect.ImmutableMap;
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
import com.jivesoftware.os.miru.service.index.MiruFilerProvider;
import com.jivesoftware.os.miru.service.locator.MiruResourcePartitionIdentifier;
import com.jivesoftware.os.miru.service.locator.MiruTempDirectoryResourceLocator;
import com.jivesoftware.os.miru.service.stream.allocator.HybridMiruContextAllocator;
import com.jivesoftware.os.miru.service.stream.allocator.MiruContextAllocator;
import com.jivesoftware.os.miru.service.stream.allocator.OnDiskMiruContextAllocator;
import com.jivesoftware.os.miru.wal.readtracking.MiruReadTrackingWALReaderImpl;
import com.jivesoftware.os.miru.wal.readtracking.hbase.MiruReadTrackingSipWALColumnKey;
import com.jivesoftware.os.miru.wal.readtracking.hbase.MiruReadTrackingWALColumnKey;
import com.jivesoftware.os.miru.wal.readtracking.hbase.MiruReadTrackingWALRow;
import com.jivesoftware.os.rcvs.inmemory.RowColumnValueStoreImpl;
import java.util.ArrayList;
import java.util.Arrays;
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
        when(config.getPartitionNumberOfChunkStores()).thenReturn(4);
        when(config.getPartitionAuthzCacheSize()).thenReturn(2);
        when(config.getPartitionDeleteChunkStoreOnClose()).thenReturn(false);
        when(config.getPartitionChunkStoreConcurrencyLevel()).thenReturn(8);
        when(config.getPartitionChunkStoreStripingLevel()).thenReturn(64);

        RowColumnValueStoreImpl<MiruTenantId, MiruReadTrackingWALRow, MiruReadTrackingWALColumnKey, MiruPartitionedActivity> readTrackingWAL =
            new RowColumnValueStoreImpl<>();
        RowColumnValueStoreImpl<MiruTenantId, MiruReadTrackingWALRow, MiruReadTrackingSipWALColumnKey, Long> readTrackingSipWAL =
            new RowColumnValueStoreImpl<>();

        schema = new MiruSchema(DefaultMiruSchemaDefinition.FIELDS);
        bitmaps = new MiruBitmapsEWAH(4);

        SingleSchemaProvider schemaProvider = new SingleSchemaProvider(schema);
        MiruActivityInternExtern activityInternExtern = new MiruActivityInternExtern(Interners.<MiruIBA>newWeakInterner(),
            Interners.<MiruTermId>newWeakInterner(),
            Interners.<MiruTenantId>newWeakInterner(), Interners.<String>newWeakInterner());
        MiruReadTrackingWALReaderImpl readTrackingWALReader = new MiruReadTrackingWALReaderImpl(readTrackingWAL, readTrackingSipWAL);

        final MiruTempDirectoryResourceLocator diskResourceLocator = new MiruTempDirectoryResourceLocator();
        final MiruTempDirectoryResourceLocator hybridResourceLocator = new MiruTempDirectoryResourceLocator();
        MiruContextAllocator hybridContextAllocator = new HybridMiruContextAllocator(schemaProvider,
            activityInternExtern,
            readTrackingWALReader,
            hybridResourceLocator,
            new HeapByteBufferFactory(),
            config.getPartitionNumberOfChunkStores(),
            config.getPartitionAuthzCacheSize(),
            config.getPartitionDeleteChunkStoreOnClose(),
            config.getPartitionChunkStoreConcurrencyLevel(),
            config.getPartitionChunkStoreStripingLevel());

        MiruContextAllocator memMappedContextAllocator = new OnDiskMiruContextAllocator("memMap",
            schemaProvider,
            activityInternExtern,
            readTrackingWALReader,
            diskResourceLocator,
            new OnDiskMiruContextAllocator.MiruFilerProviderFactory() {
                @Override
                public MiruFilerProvider getFilerProvider(MiruResourcePartitionIdentifier identifier, String name) {
                    return new OnDiskMiruContextAllocator.MemMappedFilerProvider(identifier, name, diskResourceLocator);
                }
            },
            config.getPartitionNumberOfChunkStores(),
            config.getPartitionAuthzCacheSize(),
            config.getPartitionChunkStoreConcurrencyLevel(),
            config.getPartitionChunkStoreStripingLevel());
        MiruContextAllocator diskContextAllocator = new OnDiskMiruContextAllocator("onDisk",
            schemaProvider,
            activityInternExtern,
            readTrackingWALReader,
            diskResourceLocator,
            new OnDiskMiruContextAllocator.MiruFilerProviderFactory() {
                @Override
                public MiruFilerProvider getFilerProvider(MiruResourcePartitionIdentifier identifier, String name) {
                    return new OnDiskMiruContextAllocator.OnDiskFilerProvider(identifier, name, diskResourceLocator);
                }
            },
            config.getPartitionNumberOfChunkStores(),
            config.getPartitionAuthzCacheSize(),
            config.getPartitionChunkStoreConcurrencyLevel(),
            config.getPartitionChunkStoreStripingLevel());

        streamFactory = new MiruContextFactory(
            ImmutableMap.<MiruBackingStorage, MiruContextAllocator>builder()
                .put(MiruBackingStorage.memory, hybridContextAllocator)
                .put(MiruBackingStorage.memory_fixed, hybridContextAllocator)
                .put(MiruBackingStorage.hybrid, hybridContextAllocator)
                .put(MiruBackingStorage.hybrid_fixed, hybridContextAllocator)
                .put(MiruBackingStorage.mem_mapped, memMappedContextAllocator)
                .put(MiruBackingStorage.disk, diskContextAllocator)
                .build(),
            diskResourceLocator,
            hybridResourceLocator,
            MiruBackingStorage.memory);
    }

    @Test(enabled = true, description = "This test is disk dependent, disable if it flaps or becomes slow")
    public void testCopyToDisk() throws Exception {
        int numberOfActivities = 5;

        MiruTenantId tenantId = new MiruTenantId("ace5".getBytes());
        MiruPartitionId partitionId = MiruPartitionId.of(0);
        MiruPartitionCoord coord = new MiruPartitionCoord(tenantId, partitionId, host);

        MiruIndexer<EWAHCompressedBitmap> indexer = new MiruIndexer<>(bitmaps);
        MiruContext<EWAHCompressedBitmap> inMemContext = streamFactory.allocate(bitmaps, coord, MiruBackingStorage.memory);

        for (int i = 0; i < numberOfActivities; i++) {
            String[] authz = { "aaaabbbbcccc" };
            MiruActivity activity = new MiruActivity.Builder(tenantId, (long) i, authz, 0)
                .putFieldValue(MiruFieldName.OBJECT_ID.getFieldName(), String.valueOf(i))
                .build();
            int id = inMemContext.getTimeIndex().nextId((long) i);
            indexer.index(inMemContext, new ArrayList<>(Arrays.asList(new MiruActivityAndId<>(activity, id))), MoreExecutors.sameThreadExecutor());
            indexer.remove(inMemContext, activity, id);
        }

        MiruContext<EWAHCompressedBitmap> onDiskContext = streamFactory.copy(bitmaps, coord, inMemContext, MiruBackingStorage.disk);

        assertEquals(inMemContext.getTimeIndex().getSmallestTimestamp(), onDiskContext.getTimeIndex().getSmallestTimestamp());
        assertEquals(inMemContext.getTimeIndex().getLargestTimestamp(), onDiskContext.getTimeIndex().getLargestTimestamp());

        MiruAuthzExpression authzExpression = new MiruAuthzExpression(Arrays.asList("aaaabbbbcccc"));
        assertEquals(onDiskContext.getAuthzIndex().getCompositeAuthz(authzExpression),
            inMemContext.getAuthzIndex().getCompositeAuthz(authzExpression));
        assertEquals(onDiskContext.getRemovalIndex().getIndex(),
            inMemContext.getRemovalIndex().getIndex());

        for (int i = 0; i < numberOfActivities; i++) {
            assertEquals(onDiskContext.getTimeIndex().getTimestamp(i), inMemContext.getTimeIndex().getTimestamp(i));
            assertEquals(onDiskContext.getActivityIndex().get(tenantId, i), inMemContext.getActivityIndex().get(tenantId, i));

            int fieldId = schema.getFieldId(MiruFieldName.OBJECT_ID.getFieldName());
            if (fieldId >= 0) {
                MiruTermId termId = new MiruTermId(String.valueOf(i).getBytes());
                assertEquals(onDiskContext.getFieldIndex().get(fieldId, termId).get().getIndex(),
                    inMemContext.getFieldIndex().get(fieldId, termId).get().getIndex());
            }
        }
    }

    @Test
    public void testFindBackingStorage_disk() throws Exception {
        MiruTenantId tenantId = new MiruTenantId("findBackingStorage_disk".getBytes());
        MiruPartitionId partitionId = MiruPartitionId.of(0);
        MiruPartitionCoord coord = new MiruPartitionCoord(tenantId, partitionId, host);

        MiruContext<EWAHCompressedBitmap> inMem = minimalInMemory(coord);

        MiruContext<EWAHCompressedBitmap> miruContext = streamFactory.copy(bitmaps, coord, inMem, MiruBackingStorage.disk);
        streamFactory.markStorage(coord, MiruBackingStorage.disk);
        streamFactory.close(miruContext, MiruBackingStorage.disk);

        assertEquals(streamFactory.findBackingStorage(coord), MiruBackingStorage.disk);
    }

    @Test
    public void testFindBackingStorage_memMap() throws Exception {
        MiruTenantId tenantId = new MiruTenantId("findBackingStorage_memMap".getBytes());
        MiruPartitionId partitionId = MiruPartitionId.of(0);
        MiruPartitionCoord coord = new MiruPartitionCoord(tenantId, partitionId, host);

        MiruContext<EWAHCompressedBitmap> inMem = minimalInMemory(coord);

        MiruContext<EWAHCompressedBitmap> miruContext = streamFactory.copy(bitmaps, coord, inMem, MiruBackingStorage.mem_mapped);
        streamFactory.markStorage(coord, MiruBackingStorage.mem_mapped);
        streamFactory.close(miruContext, MiruBackingStorage.mem_mapped);

        assertEquals(streamFactory.findBackingStorage(coord), MiruBackingStorage.mem_mapped);
    }

    private MiruContext<EWAHCompressedBitmap> minimalInMemory(MiruPartitionCoord coord) throws Exception {
        //TODO detecting backing storage fails if we haven't indexed at least 1 term for every field, 1 inbox, 1 unread
        MiruActivity.Builder builder = new MiruActivity.Builder(coord.tenantId, 0, new String[] { "abcd" }, 0);
        for (MiruFieldName fieldName : MiruFieldName.values()) {
            builder.putFieldValue(fieldName.getFieldName(), "defg");
        }

        MiruIndexer<EWAHCompressedBitmap> indexer = new MiruIndexer<>(bitmaps);
        MiruContext<EWAHCompressedBitmap> inMemContext = streamFactory.allocate(bitmaps, coord, MiruBackingStorage.memory);
        int id = inMemContext.getTimeIndex().nextId(System.currentTimeMillis());
        MiruStreamId streamId = new MiruStreamId(FilerIO.longBytes(0));
        indexer.index(inMemContext, new ArrayList<>(Arrays.asList(new MiruActivityAndId<>(builder.build(), id))), MoreExecutors.sameThreadExecutor());
        inMemContext.getInboxIndex().index(streamId, id);
        inMemContext.getUnreadTrackingIndex().index(streamId, id);
        return inMemContext;
    }
}
