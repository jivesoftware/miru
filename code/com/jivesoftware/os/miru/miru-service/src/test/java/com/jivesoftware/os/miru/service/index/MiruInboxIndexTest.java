package com.jivesoftware.os.miru.service.index;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.googlecode.javaewah.EWAHCompressedBitmap;
import com.jivesoftware.os.jive.utils.id.Id;
import com.jivesoftware.os.miru.api.MiruHost;
import com.jivesoftware.os.miru.api.MiruPartitionCoord;
import com.jivesoftware.os.miru.api.activity.MiruPartitionId;
import com.jivesoftware.os.miru.api.base.MiruStreamId;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.plugin.index.MiruInboxIndex;
import com.jivesoftware.os.miru.plugin.index.MiruInvertedIndexAppender;
import com.jivesoftware.os.miru.service.IndexTestUtil;
import com.jivesoftware.os.miru.service.bitmap.MiruBitmapsEWAH;
import com.jivesoftware.os.miru.service.stream.MiruContext;
import java.util.Collections;
import java.util.List;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

public class MiruInboxIndexTest {

    @Test(dataProvider = "miruInboxIndexDataProvider")
    public void testGetEmptyInboxWithoutCreating(MiruInboxIndex<EWAHCompressedBitmap> miruInboxIndex, MiruStreamId miruStreamId)
        throws Exception {

        Optional<EWAHCompressedBitmap> inbox = miruInboxIndex.getInbox(miruStreamId);
        assertNotNull(inbox);
        assertFalse(inbox.isPresent());
    }

    @Test(dataProvider = "miruInboxIndexDataProvider")
    public void testGetEmptyInboxAndCreate(MiruInboxIndex<EWAHCompressedBitmap> miruInboxIndex, MiruStreamId miruStreamId)
        throws Exception {

        MiruInvertedIndexAppender inbox = miruInboxIndex.getAppender(miruStreamId);
        assertNotNull(inbox);
        Optional<EWAHCompressedBitmap> inboxIndex = miruInboxIndex.getInbox(miruStreamId);
        assertNotNull(inboxIndex);
        assertFalse(inboxIndex.isPresent());
        //assertEquals(inboxIndex.get().sizeInBytes(), 8);
    }

    @Test(dataProvider = "miruInboxIndexDataProvider")
    public void testIndexIds(MiruInboxIndex miruInboxIndex, MiruStreamId miruStreamId) throws Exception {
        miruInboxIndex.index(miruStreamId, 1);
    }

    @Test(dataProvider = "miruInboxIndexDataProviderWithData")
    public void testIndexedData(MiruInboxIndex<EWAHCompressedBitmap> miruInboxIndex, MiruStreamId streamId, List<Integer> indexedIds) throws Exception {
        Optional<EWAHCompressedBitmap> inbox = miruInboxIndex.getInbox(streamId);
        assertNotNull(inbox);
        assertTrue(inbox.isPresent());
        assertTrue(inbox.get().get(indexedIds.get(0)));
        assertTrue(inbox.get().get(indexedIds.get(1)));
        assertTrue(inbox.get().get(indexedIds.get(2)));
    }

    @Test(dataProvider = "miruInboxIndexDataProviderWithData")
    public void testDefaultLastActivityIndexWithNewStreamId(MiruInboxIndex<EWAHCompressedBitmap> miruInboxIndex, MiruStreamId streamId,
        List<Integer> indexedIds) throws Exception {

        MiruStreamId newStreamId = new MiruStreamId(new Id(1_337).toBytes());
        int lastActivityIndex = miruInboxIndex.getLastActivityIndex(newStreamId);
        assertEquals(lastActivityIndex, -1);

        int nextId = Integer.MAX_VALUE / 2;
        miruInboxIndex.index(newStreamId, nextId);
        lastActivityIndex = miruInboxIndex.getLastActivityIndex(newStreamId);
        assertEquals(lastActivityIndex, nextId);
    }

    @Test(dataProvider = "miruInboxIndexDataProviderWithData")
    public void testLastActivityIndex(MiruInboxIndex<EWAHCompressedBitmap> miruInboxIndex, MiruStreamId streamId, List<Integer> indexedIds) throws Exception {

        int expected = miruInboxIndex.getLastActivityIndex(streamId);
        assertEquals(expected, indexedIds.get(indexedIds.size() - 1).intValue());

        int nextId = Integer.MAX_VALUE / 3;
        miruInboxIndex.getAppender(streamId).appendAndExtend(Collections.<Integer>emptyList(), nextId);
        int lastActivityIndex = miruInboxIndex.getLastActivityIndex(streamId);
        assertEquals(lastActivityIndex, expected);
    }

    @DataProvider(name = "miruInboxIndexDataProvider")
    public Object[][] miruInboxIndexDataProvider() throws Exception {
        MiruStreamId miruStreamId = new MiruStreamId(new Id(12_345).toBytes());
        MiruTenantId tenantId = new MiruTenantId(new byte[] { 1 });
        MiruPartitionCoord coord = new MiruPartitionCoord(tenantId, MiruPartitionId.of(0), new MiruHost("localhost", 10000));
        MiruBitmapsEWAH bitmaps = new MiruBitmapsEWAH(10);

        MiruContext<EWAHCompressedBitmap> hybridContext = IndexTestUtil.buildHybridContextAllocator(4, 10, true, 64).allocate(bitmaps, coord);
        MiruInboxIndex<EWAHCompressedBitmap> miruInMemoryInboxIndex = hybridContext.inboxIndex;

        MiruContext<EWAHCompressedBitmap> onDiskContext = IndexTestUtil.buildOnDiskContextAllocator(4, 10, 64).allocate(bitmaps, coord);
        MiruInboxIndex<EWAHCompressedBitmap> miruOnDiskInboxIndex = onDiskContext.inboxIndex;

        ((BulkImport) miruOnDiskInboxIndex).bulkImport(tenantId, (BulkExport) miruInMemoryInboxIndex);

        return new Object[][] {
            { miruInMemoryInboxIndex, miruStreamId },
            { miruOnDiskInboxIndex, miruStreamId }
        };
    }

    @DataProvider(name = "miruInboxIndexDataProviderWithData")
    public Object[][] miruInboxIndexDataProviderWithData() throws Exception {
        MiruStreamId streamId = new MiruStreamId(new Id(12_345).toBytes());

        // Create in-memory inbox index
        MiruBitmapsEWAH bitmaps = new MiruBitmapsEWAH(10);
        MiruTenantId tenantId = new MiruTenantId(new byte[] { 1 });
        MiruPartitionCoord coord = new MiruPartitionCoord(tenantId, MiruPartitionId.of(0), new MiruHost("localhost", 10000));

        MiruContext<EWAHCompressedBitmap> hybridContext = IndexTestUtil.buildHybridContextAllocator(4, 10, true, 64).allocate(bitmaps, coord);
        MiruInboxIndex<EWAHCompressedBitmap> miruInMemoryInboxIndex = hybridContext.inboxIndex;

        miruInMemoryInboxIndex.index(streamId, 1);
        miruInMemoryInboxIndex.index(streamId, 2);
        miruInMemoryInboxIndex.index(streamId, 3);

        MiruContext<EWAHCompressedBitmap> onDiskContext = IndexTestUtil.buildOnDiskContextAllocator(4, 10, 64).allocate(bitmaps, coord);
        MiruInboxIndex<EWAHCompressedBitmap> miruOnDiskInboxIndex = onDiskContext.inboxIndex;

        ((BulkImport) miruOnDiskInboxIndex).bulkImport(tenantId, (BulkExport) miruInMemoryInboxIndex);

        return new Object[][] {
            { miruInMemoryInboxIndex, streamId, ImmutableList.of(1, 2, 3) },
            { miruOnDiskInboxIndex, streamId, ImmutableList.of(1, 2, 3) }
        };
    }
}
