package com.jivesoftware.os.miru.service.index;

import com.google.common.base.Optional;
import com.jivesoftware.os.filer.io.api.StackBuffer;
import com.jivesoftware.os.miru.api.MiruHost;
import com.jivesoftware.os.miru.api.MiruPartitionCoord;
import com.jivesoftware.os.miru.api.activity.MiruPartitionId;
import com.jivesoftware.os.miru.api.activity.schema.MiruSchema;
import com.jivesoftware.os.miru.api.activity.schema.MiruSchema.Builder;
import com.jivesoftware.os.miru.api.base.MiruStreamId;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.api.wal.RCVSSipCursor;
import com.jivesoftware.os.miru.bitmaps.roaring5.MiruBitmapsRoaring;
import com.jivesoftware.os.miru.plugin.bitmap.MiruBitmaps;
import com.jivesoftware.os.miru.plugin.index.MiruInboxIndex;
import com.jivesoftware.os.miru.plugin.index.MiruInvertedIndexAppender;
import com.jivesoftware.os.miru.service.IndexTestUtil;
import com.jivesoftware.os.miru.service.stream.MiruContext;
import org.apache.commons.lang3.ArrayUtils;
import org.roaringbitmap.RoaringBitmap;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

public class MiruInboxIndexTest {

    @Test(dataProvider = "miruInboxIndexDataProvider")
    public void testGetEmptyInboxWithoutCreating(MiruInboxIndex<RoaringBitmap, RoaringBitmap> miruInboxIndex, MiruStreamId miruStreamId)
        throws Exception {
        StackBuffer stackBuffer = new StackBuffer();
        Optional<RoaringBitmap> inbox = miruInboxIndex.getInbox(miruStreamId).getIndex(stackBuffer);
        assertNotNull(inbox);
        assertFalse(inbox.isPresent());
    }

    @Test(dataProvider = "miruInboxIndexDataProvider")
    public void testGetEmptyInboxAndCreate(MiruInboxIndex<RoaringBitmap, RoaringBitmap> miruInboxIndex, MiruStreamId miruStreamId)
        throws Exception {
        StackBuffer stackBuffer = new StackBuffer();
        MiruInvertedIndexAppender inbox = miruInboxIndex.getAppender(miruStreamId);
        assertNotNull(inbox);
        Optional<RoaringBitmap> inboxIndex = miruInboxIndex.getInbox(miruStreamId).getIndex(stackBuffer);
        assertNotNull(inboxIndex);
        assertFalse(inboxIndex.isPresent());
        //assertEquals(inboxIndex.get().sizeInBytes(), 8);
    }

    @Test(dataProvider = "miruInboxIndexDataProvider")
    public void testIndexIds(MiruInboxIndex miruInboxIndex, MiruStreamId miruStreamId) throws Exception {
        StackBuffer stackBuffer = new StackBuffer();
        miruInboxIndex.append(miruStreamId, stackBuffer, 1);
    }

    @Test(dataProvider = "miruInboxIndexDataProviderWithData")
    public void testIndexedData(MiruInboxIndex<RoaringBitmap, RoaringBitmap> miruInboxIndex,
        MiruStreamId streamId,
        int[] indexedIds) throws Exception {
        StackBuffer stackBuffer = new StackBuffer();
        Optional<RoaringBitmap> inbox = miruInboxIndex.getInbox(streamId).getIndex(stackBuffer);
        assertNotNull(inbox);
        assertTrue(inbox.isPresent());
        for (int indexedId : indexedIds) {
            assertTrue(inbox.get().contains(indexedId));
        }
    }

    @Test(dataProvider = "miruInboxIndexDataProviderWithData")
    public void testDefaultLastActivityIndexWithNewStreamId(MiruInboxIndex<RoaringBitmap, RoaringBitmap> miruInboxIndex,
        MiruStreamId streamId,
        int[] indexedIds) throws Exception {
        StackBuffer stackBuffer = new StackBuffer();
        MiruStreamId newStreamId = new MiruStreamId(new byte[] { 1 });
        int lastActivityIndex = miruInboxIndex.getLastActivityIndex(newStreamId, stackBuffer);
        assertEquals(lastActivityIndex, -1);

        int nextId = Integer.MAX_VALUE / 2;
        miruInboxIndex.append(newStreamId, stackBuffer, nextId);
        lastActivityIndex = miruInboxIndex.getLastActivityIndex(newStreamId, stackBuffer);
        assertEquals(lastActivityIndex, nextId);
    }

    @DataProvider(name = "miruInboxIndexDataProvider")
    public Object[][] miruInboxIndexDataProvider() throws Exception {
        MiruStreamId miruStreamId = new MiruStreamId(new byte[] { 2 });
        MiruTenantId tenantId = new MiruTenantId(new byte[] { 1 });
        MiruPartitionCoord coord = new MiruPartitionCoord(tenantId, MiruPartitionId.of(0), new MiruHost("logicalName"));
        MiruBitmapsRoaring bitmaps = new MiruBitmapsRoaring();
        return ArrayUtils.addAll(buildInboxIndexDataProvider(miruStreamId, coord, bitmaps, false),
            buildInboxIndexDataProvider(miruStreamId, coord, bitmaps, true));
    }

    private <BM extends IBM, IBM> Object[][] buildInboxIndexDataProvider(MiruStreamId miruStreamId,
        MiruPartitionCoord coord,
        MiruBitmaps<BM, IBM> bitmaps,
        boolean useLabIndexes) throws Exception {

        MiruContext<BM, IBM, RCVSSipCursor> inMemoryContext = IndexTestUtil.buildInMemoryContext(4, useLabIndexes, true, bitmaps, coord);
        MiruInboxIndex<BM, IBM> inMemoryIndex = inMemoryContext.inboxIndex;

        MiruContext<BM, IBM, ?> onDiskContext = IndexTestUtil.buildOnDiskContext(4, useLabIndexes, true, bitmaps, coord);
        MiruInboxIndex<BM, IBM> onDiskIndex = onDiskContext.inboxIndex;

        return new Object[][] {
            { inMemoryIndex, miruStreamId },
            { onDiskIndex, miruStreamId }
        };
    }

    @DataProvider(name = "miruInboxIndexDataProviderWithData")
    public Object[][] miruInboxIndexDataProviderWithData() throws Exception {
        StackBuffer stackBuffer = new StackBuffer();
        MiruSchema schema = new Builder("test", 1).build();
        MiruStreamId streamId = new MiruStreamId(new byte[] { 3 });

        // Create in-memory inbox index
        MiruBitmapsRoaring bitmaps = new MiruBitmapsRoaring();
        MiruTenantId tenantId = new MiruTenantId(new byte[] { 1 });
        MiruPartitionCoord coord = new MiruPartitionCoord(tenantId, MiruPartitionId.of(0), new MiruHost("logicalName"));

        return ArrayUtils.addAll(buildInboxIndexDataProviderWithData(stackBuffer, schema, streamId, bitmaps, coord, false),
            buildInboxIndexDataProviderWithData(stackBuffer, schema, streamId, bitmaps, coord, true));
    }

    private <BM extends IBM, IBM> Object[][] buildInboxIndexDataProviderWithData(StackBuffer stackBuffer,
        MiruSchema schema,
        MiruStreamId streamId,
        MiruBitmaps<BM, IBM> bitmaps,
        MiruPartitionCoord coord,
        boolean useLabIndexes) throws Exception {

        int[] data = new int[] { 1, 2, 3, 4 };

        //TODO unnecessary casts, but the wildcards cause some IDE confusion
        MiruContext<BM, IBM, RCVSSipCursor> unmergedInMemoryContext = IndexTestUtil.buildInMemoryContext(4,
            useLabIndexes,
            true,
            bitmaps,
            coord);
        MiruInboxIndex<BM, IBM> unmergedInMemoryIndex = unmergedInMemoryContext.inboxIndex;
        unmergedInMemoryIndex.append(streamId, stackBuffer, data);

        MiruContext<BM, IBM, RCVSSipCursor> unmergedOnDiskContext = IndexTestUtil.buildOnDiskContext(4,
            useLabIndexes,
            true,
            bitmaps,
            coord);
        MiruInboxIndex<BM, IBM> unmergedOnDiskIndex = unmergedOnDiskContext.inboxIndex;
        unmergedOnDiskIndex.append(streamId, stackBuffer, data);

        return new Object[][] {
            { unmergedInMemoryIndex, streamId, data },
            { unmergedOnDiskIndex, streamId, data }
        };
    }
}
