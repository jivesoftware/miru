package com.jivesoftware.os.miru.service.index;

import com.google.common.base.Optional;
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
import com.jivesoftware.os.miru.service.index.delta.MiruDeltaInboxIndex;
import com.jivesoftware.os.miru.service.stream.MiruContext;
import java.util.Collections;
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

        Optional<EWAHCompressedBitmap> inbox = miruInboxIndex.getInbox(miruStreamId).getIndex();
        assertNotNull(inbox);
        assertFalse(inbox.isPresent());
    }

    @Test(dataProvider = "miruInboxIndexDataProvider")
    public void testGetEmptyInboxAndCreate(MiruInboxIndex<EWAHCompressedBitmap> miruInboxIndex, MiruStreamId miruStreamId)
        throws Exception {

        MiruInvertedIndexAppender inbox = miruInboxIndex.getAppender(miruStreamId);
        assertNotNull(inbox);
        Optional<EWAHCompressedBitmap> inboxIndex = miruInboxIndex.getInbox(miruStreamId).getIndex();
        assertNotNull(inboxIndex);
        assertFalse(inboxIndex.isPresent());
        //assertEquals(inboxIndex.get().sizeInBytes(), 8);
    }

    @Test(dataProvider = "miruInboxIndexDataProvider")
    public void testIndexIds(MiruInboxIndex miruInboxIndex, MiruStreamId miruStreamId) throws Exception {
        miruInboxIndex.append(miruStreamId, 1);
    }

    @Test(dataProvider = "miruInboxIndexDataProviderWithData")
    public void testIndexedData(MiruInboxIndex<EWAHCompressedBitmap> miruInboxIndex, MiruStreamId streamId, int[] indexedIds) throws Exception {
        Optional<EWAHCompressedBitmap> inbox = miruInboxIndex.getInbox(streamId).getIndex();
        assertNotNull(inbox);
        assertTrue(inbox.isPresent());
        for (int indexedId : indexedIds) {
            assertTrue(inbox.get().get(indexedId));
        }
    }

    @Test(dataProvider = "miruInboxIndexDataProviderWithData")
    public void testDefaultLastActivityIndexWithNewStreamId(MiruInboxIndex<EWAHCompressedBitmap> miruInboxIndex, MiruStreamId streamId,
        int[] indexedIds) throws Exception {

        MiruStreamId newStreamId = new MiruStreamId(new Id(1_337).toBytes());
        int lastActivityIndex = miruInboxIndex.getLastActivityIndex(newStreamId);
        assertEquals(lastActivityIndex, -1);

        int nextId = Integer.MAX_VALUE / 2;
        miruInboxIndex.append(newStreamId, nextId);
        lastActivityIndex = miruInboxIndex.getLastActivityIndex(newStreamId);
        assertEquals(lastActivityIndex, nextId);
    }

    @Test(dataProvider = "miruInboxIndexDataProviderWithData")
    public void testLastActivityIndex(MiruInboxIndex<EWAHCompressedBitmap> miruInboxIndex, MiruStreamId streamId, int[] indexedIds) throws Exception {

        int expected = miruInboxIndex.getLastActivityIndex(streamId);
        assertEquals(expected, indexedIds[indexedIds.length - 1]);

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

        MiruContext<EWAHCompressedBitmap, ?> inMemoryContext = IndexTestUtil.buildInMemoryContext(4, bitmaps, coord);
        MiruInboxIndex<EWAHCompressedBitmap> inMemoryIndex = inMemoryContext.inboxIndex;

        MiruContext<EWAHCompressedBitmap, ?> onDiskContext = IndexTestUtil.buildOnDiskContext(4, bitmaps, coord);
        MiruInboxIndex<EWAHCompressedBitmap> onDiskIndex = onDiskContext.inboxIndex;

        return new Object[][] {
            { inMemoryIndex, miruStreamId },
            { onDiskIndex, miruStreamId }
        };
    }

    @DataProvider(name = "miruInboxIndexDataProviderWithData")
    public Object[][] miruInboxIndexDataProviderWithData() throws Exception {
        MiruStreamId streamId = new MiruStreamId(new Id(12_345).toBytes());

        // Create in-memory inbox index
        MiruBitmapsEWAH bitmaps = new MiruBitmapsEWAH(10);
        MiruTenantId tenantId = new MiruTenantId(new byte[] { 1 });
        MiruPartitionCoord coord = new MiruPartitionCoord(tenantId, MiruPartitionId.of(0), new MiruHost("localhost", 10000));

        int[] data = new int[] { 1, 2, 3, 4 };
        int[] data1_2 = new int[] { 1, 2 };
        int[] data2_2 = new int[] { 3, 4 };

        MiruContext<EWAHCompressedBitmap, ?> unmergedInMemoryContext = IndexTestUtil.buildInMemoryContext(4, bitmaps, coord);
        MiruInboxIndex<EWAHCompressedBitmap> unmergedInMemoryIndex = unmergedInMemoryContext.inboxIndex;
        unmergedInMemoryIndex.append(streamId, data);

        MiruContext<EWAHCompressedBitmap, ?> unmergedOnDiskContext = IndexTestUtil.buildOnDiskContext(4, bitmaps, coord);
        MiruInboxIndex<EWAHCompressedBitmap> unmergedOnDiskIndex = unmergedOnDiskContext.inboxIndex;
        unmergedOnDiskIndex.append(streamId, data);

        MiruContext<EWAHCompressedBitmap, ?> mergedInMemoryContext = IndexTestUtil.buildInMemoryContext(4, bitmaps, coord);
        MiruInboxIndex<EWAHCompressedBitmap> mergedInMemoryIndex = mergedInMemoryContext.inboxIndex;
        mergedInMemoryIndex.append(streamId, data);
        ((MiruDeltaInboxIndex<EWAHCompressedBitmap>) mergedInMemoryIndex).merge();

        MiruContext<EWAHCompressedBitmap, ?> mergedOnDiskContext = IndexTestUtil.buildOnDiskContext(4, bitmaps, coord);
        MiruInboxIndex<EWAHCompressedBitmap> mergedOnDiskIndex = mergedOnDiskContext.inboxIndex;
        mergedOnDiskIndex.append(streamId, data);
        ((MiruDeltaInboxIndex<EWAHCompressedBitmap>) mergedOnDiskIndex).merge();

        MiruContext<EWAHCompressedBitmap, ?> partiallyMergedInMemoryContext = IndexTestUtil.buildInMemoryContext(4, bitmaps, coord);
        MiruInboxIndex<EWAHCompressedBitmap> partiallyMergedInMemoryIndex = partiallyMergedInMemoryContext.inboxIndex;
        partiallyMergedInMemoryIndex.append(streamId, data1_2);
        ((MiruDeltaInboxIndex<EWAHCompressedBitmap>) partiallyMergedInMemoryIndex).merge();
        partiallyMergedInMemoryIndex.append(streamId, data2_2);

        MiruContext<EWAHCompressedBitmap, ?> partiallyMergedOnDiskContext = IndexTestUtil.buildOnDiskContext(4, bitmaps, coord);
        MiruInboxIndex<EWAHCompressedBitmap> partiallyMergedOnDiskIndex = partiallyMergedOnDiskContext.inboxIndex;
        partiallyMergedOnDiskIndex.append(streamId, data1_2);
        ((MiruDeltaInboxIndex<EWAHCompressedBitmap>) partiallyMergedOnDiskIndex).merge();
        partiallyMergedOnDiskIndex.append(streamId, data2_2);

        return new Object[][] {
            { unmergedInMemoryIndex, streamId, data },
            { unmergedOnDiskIndex, streamId, data },
            { mergedInMemoryIndex, streamId, data },
            { mergedOnDiskIndex, streamId, data },
            { partiallyMergedInMemoryIndex, streamId, data },
            { partiallyMergedOnDiskIndex, streamId, data }
        };
    }
}
