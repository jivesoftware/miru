package com.jivesoftware.os.miru.service.index;

import com.google.common.base.Optional;
import com.jivesoftware.os.filer.io.api.StackBuffer;
import com.jivesoftware.os.miru.api.MiruHost;
import com.jivesoftware.os.miru.api.MiruPartitionCoord;
import com.jivesoftware.os.miru.api.activity.MiruPartitionId;
import com.jivesoftware.os.miru.api.base.MiruStreamId;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.bitmaps.roaring5.buffer.MiruBitmapsRoaringBuffer;
import com.jivesoftware.os.miru.plugin.bitmap.MiruBitmaps;
import com.jivesoftware.os.miru.plugin.index.MiruInboxIndex;
import com.jivesoftware.os.miru.plugin.index.MiruInvertedIndexAppender;
import com.jivesoftware.os.miru.service.IndexTestUtil;
import com.jivesoftware.os.miru.service.index.delta.MiruDeltaInboxIndex;
import com.jivesoftware.os.miru.service.stream.MiruContext;
import java.util.Collections;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;
import org.roaringbitmap.buffer.MutableRoaringBitmap;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

public class MiruInboxIndexTest {

    @Test(dataProvider = "miruInboxIndexDataProvider")
    public void testGetEmptyInboxWithoutCreating(MiruInboxIndex<MutableRoaringBitmap, ImmutableRoaringBitmap> miruInboxIndex, MiruStreamId miruStreamId)
        throws Exception {
        StackBuffer stackBuffer = new StackBuffer();
        Optional<MutableRoaringBitmap> inbox = miruInboxIndex.getInbox(miruStreamId).getIndex(stackBuffer);
        assertNotNull(inbox);
        assertFalse(inbox.isPresent());
    }

    @Test(dataProvider = "miruInboxIndexDataProvider")
    public void testGetEmptyInboxAndCreate(MiruInboxIndex<MutableRoaringBitmap, ImmutableRoaringBitmap> miruInboxIndex, MiruStreamId miruStreamId)
        throws Exception {
        StackBuffer stackBuffer = new StackBuffer();
        MiruInvertedIndexAppender inbox = miruInboxIndex.getAppender(miruStreamId);
        assertNotNull(inbox);
        Optional<MutableRoaringBitmap> inboxIndex = miruInboxIndex.getInbox(miruStreamId).getIndex(stackBuffer);
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
    public void testIndexedData(MiruInboxIndex<MutableRoaringBitmap, ImmutableRoaringBitmap> miruInboxIndex,
        MiruStreamId streamId,
        int[] indexedIds) throws Exception {
        StackBuffer stackBuffer = new StackBuffer();
        Optional<MutableRoaringBitmap> inbox = miruInboxIndex.getInbox(streamId).getIndex(stackBuffer);
        assertNotNull(inbox);
        assertTrue(inbox.isPresent());
        for (int indexedId : indexedIds) {
            assertTrue(inbox.get().contains(indexedId));
        }
    }

    @Test(dataProvider = "miruInboxIndexDataProviderWithData")
    public void testDefaultLastActivityIndexWithNewStreamId(MiruInboxIndex<MutableRoaringBitmap, ImmutableRoaringBitmap> miruInboxIndex, MiruStreamId streamId,
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

    @Test(dataProvider = "miruInboxIndexDataProviderWithData")
    public void testLastActivityIndex(MiruInboxIndex<MutableRoaringBitmap, ImmutableRoaringBitmap> miruInboxIndex,
        MiruStreamId streamId,
        int[] indexedIds) throws Exception {
        StackBuffer stackBuffer = new StackBuffer();
        int expected = miruInboxIndex.getLastActivityIndex(streamId, stackBuffer);
        assertEquals(expected, indexedIds[indexedIds.length - 1]);

        int nextId = Integer.MAX_VALUE / 3;
        miruInboxIndex.getAppender(streamId).appendAndExtend(Collections.<Integer>emptyList(), nextId, stackBuffer);
        int lastActivityIndex = miruInboxIndex.getLastActivityIndex(streamId, stackBuffer);
        assertEquals(lastActivityIndex, expected);
    }

    @DataProvider(name = "miruInboxIndexDataProvider")
    public Object[][] miruInboxIndexDataProvider() throws Exception {
        MiruStreamId miruStreamId = new MiruStreamId(new byte[] { 2 });
        MiruTenantId tenantId = new MiruTenantId(new byte[] { 1 });
        MiruPartitionCoord coord = new MiruPartitionCoord(tenantId, MiruPartitionId.of(0), new MiruHost("logicalName"));
        MiruBitmapsRoaringBuffer bitmaps = new MiruBitmapsRoaringBuffer();

        //TODO unnecessary casts, but the wildcards cause some IDE confusion
        MiruContext<MutableRoaringBitmap, ImmutableRoaringBitmap, ?> inMemoryContext = IndexTestUtil.buildInMemoryContext(4, (MiruBitmaps) bitmaps, coord);
        MiruInboxIndex<MutableRoaringBitmap, ImmutableRoaringBitmap> inMemoryIndex = inMemoryContext.inboxIndex;

        MiruContext<MutableRoaringBitmap, ImmutableRoaringBitmap, ?> onDiskContext = IndexTestUtil.buildOnDiskContext(4, (MiruBitmaps) bitmaps, coord);
        MiruInboxIndex<MutableRoaringBitmap, ImmutableRoaringBitmap> onDiskIndex = onDiskContext.inboxIndex;

        return new Object[][] {
            { inMemoryIndex, miruStreamId },
            { onDiskIndex, miruStreamId }
        };
    }

    @DataProvider(name = "miruInboxIndexDataProviderWithData")
    public Object[][] miruInboxIndexDataProviderWithData() throws Exception {
        StackBuffer stackBuffer = new StackBuffer();
        MiruStreamId streamId = new MiruStreamId(new byte[] { 3 });

        // Create in-memory inbox index
        MiruBitmapsRoaringBuffer bitmaps = new MiruBitmapsRoaringBuffer();
        MiruTenantId tenantId = new MiruTenantId(new byte[] { 1 });
        MiruPartitionCoord coord = new MiruPartitionCoord(tenantId, MiruPartitionId.of(0), new MiruHost("logicalName"));

        int[] data = new int[] { 1, 2, 3, 4 };
        int[] data1_2 = new int[] { 1, 2 };
        int[] data2_2 = new int[] { 3, 4 };

        //TODO unnecessary casts, but the wildcards cause some IDE confusion
        MiruContext<MutableRoaringBitmap, ImmutableRoaringBitmap, ?> unmergedInMemoryContext = IndexTestUtil.buildInMemoryContext(4, (MiruBitmaps) bitmaps,
            coord);
        MiruInboxIndex<MutableRoaringBitmap, ImmutableRoaringBitmap> unmergedInMemoryIndex = unmergedInMemoryContext.inboxIndex;
        unmergedInMemoryIndex.append(streamId, stackBuffer, data);

        MiruContext<MutableRoaringBitmap, ImmutableRoaringBitmap, ?> unmergedOnDiskContext = IndexTestUtil.buildOnDiskContext(4, (MiruBitmaps) bitmaps, coord);
        MiruInboxIndex<MutableRoaringBitmap, ImmutableRoaringBitmap> unmergedOnDiskIndex = unmergedOnDiskContext.inboxIndex;
        unmergedOnDiskIndex.append(streamId, stackBuffer, data);

        MiruContext<MutableRoaringBitmap, ImmutableRoaringBitmap, ?> mergedInMemoryContext = IndexTestUtil.buildInMemoryContext(4, (MiruBitmaps) bitmaps,
            coord);
        MiruInboxIndex<MutableRoaringBitmap, ImmutableRoaringBitmap> mergedInMemoryIndex = mergedInMemoryContext.inboxIndex;
        mergedInMemoryIndex.append(streamId, stackBuffer, data);
        ((MiruDeltaInboxIndex<MutableRoaringBitmap, ImmutableRoaringBitmap>) mergedInMemoryIndex).merge(stackBuffer);

        MiruContext<MutableRoaringBitmap, ImmutableRoaringBitmap, ?> mergedOnDiskContext = IndexTestUtil.buildOnDiskContext(4, (MiruBitmaps) bitmaps, coord);
        MiruInboxIndex<MutableRoaringBitmap, ImmutableRoaringBitmap> mergedOnDiskIndex = mergedOnDiskContext.inboxIndex;
        mergedOnDiskIndex.append(streamId, stackBuffer, data);
        ((MiruDeltaInboxIndex<MutableRoaringBitmap, ImmutableRoaringBitmap>) mergedOnDiskIndex).merge(stackBuffer);

        MiruContext<MutableRoaringBitmap, ImmutableRoaringBitmap, ?> partiallyMergedInMemoryContext = IndexTestUtil.buildInMemoryContext(4,
            (MiruBitmaps) bitmaps, coord);
        MiruInboxIndex<MutableRoaringBitmap, ImmutableRoaringBitmap> partiallyMergedInMemoryIndex = partiallyMergedInMemoryContext.inboxIndex;
        partiallyMergedInMemoryIndex.append(streamId, stackBuffer, data1_2);
        ((MiruDeltaInboxIndex<MutableRoaringBitmap, ImmutableRoaringBitmap>) partiallyMergedInMemoryIndex).merge(stackBuffer);
        partiallyMergedInMemoryIndex.append(streamId, stackBuffer, data2_2);

        MiruContext<MutableRoaringBitmap, ImmutableRoaringBitmap, ?> partiallyMergedOnDiskContext = IndexTestUtil.buildOnDiskContext(4, (MiruBitmaps) bitmaps,
            coord);
        MiruInboxIndex<MutableRoaringBitmap, ImmutableRoaringBitmap> partiallyMergedOnDiskIndex = partiallyMergedOnDiskContext.inboxIndex;
        partiallyMergedOnDiskIndex.append(streamId, stackBuffer, data1_2);
        ((MiruDeltaInboxIndex<MutableRoaringBitmap, ImmutableRoaringBitmap>) partiallyMergedOnDiskIndex).merge(stackBuffer);
        partiallyMergedOnDiskIndex.append(streamId, stackBuffer, data2_2);

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
