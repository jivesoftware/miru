package com.jivesoftware.os.miru.service.index;

import com.google.common.base.Optional;
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
    public void testGetEmptyInboxWithoutCreating(MiruInboxIndex<ImmutableRoaringBitmap> miruInboxIndex, MiruStreamId miruStreamId)
        throws Exception {
        byte[] primitiveBuffer = new byte[8];
        Optional<ImmutableRoaringBitmap> inbox = miruInboxIndex.getInbox(miruStreamId).getIndex(primitiveBuffer);
        assertNotNull(inbox);
        assertFalse(inbox.isPresent());
    }

    @Test(dataProvider = "miruInboxIndexDataProvider")
    public void testGetEmptyInboxAndCreate(MiruInboxIndex<ImmutableRoaringBitmap> miruInboxIndex, MiruStreamId miruStreamId)
        throws Exception {
        byte[] primitiveBuffer = new byte[8];
        MiruInvertedIndexAppender inbox = miruInboxIndex.getAppender(miruStreamId);
        assertNotNull(inbox);
        Optional<ImmutableRoaringBitmap> inboxIndex = miruInboxIndex.getInbox(miruStreamId).getIndex(primitiveBuffer);
        assertNotNull(inboxIndex);
        assertFalse(inboxIndex.isPresent());
        //assertEquals(inboxIndex.get().sizeInBytes(), 8);
    }

    @Test(dataProvider = "miruInboxIndexDataProvider")
    public void testIndexIds(MiruInboxIndex miruInboxIndex, MiruStreamId miruStreamId) throws Exception {
        byte[] primitiveBuffer = new byte[8];
        miruInboxIndex.append(miruStreamId, primitiveBuffer, 1);
    }

    @Test(dataProvider = "miruInboxIndexDataProviderWithData")
    public void testIndexedData(MiruInboxIndex<ImmutableRoaringBitmap> miruInboxIndex, MiruStreamId streamId, int[] indexedIds) throws Exception {
        byte[] primitiveBuffer = new byte[8];
        Optional<ImmutableRoaringBitmap> inbox = miruInboxIndex.getInbox(streamId).getIndex(primitiveBuffer);
        assertNotNull(inbox);
        assertTrue(inbox.isPresent());
        for (int indexedId : indexedIds) {
            assertTrue(inbox.get().contains(indexedId));
        }
    }

    @Test(dataProvider = "miruInboxIndexDataProviderWithData")
    public void testDefaultLastActivityIndexWithNewStreamId(MiruInboxIndex<MutableRoaringBitmap> miruInboxIndex, MiruStreamId streamId,
        int[] indexedIds) throws Exception {
        byte[] primitiveBuffer = new byte[8];
        MiruStreamId newStreamId = new MiruStreamId(new byte[] { 1 });
        int lastActivityIndex = miruInboxIndex.getLastActivityIndex(newStreamId, primitiveBuffer);
        assertEquals(lastActivityIndex, -1);

        int nextId = Integer.MAX_VALUE / 2;
        miruInboxIndex.append(newStreamId, primitiveBuffer, nextId);
        lastActivityIndex = miruInboxIndex.getLastActivityIndex(newStreamId, primitiveBuffer);
        assertEquals(lastActivityIndex, nextId);
    }

    @Test(dataProvider = "miruInboxIndexDataProviderWithData")
    public void testLastActivityIndex(MiruInboxIndex<MutableRoaringBitmap> miruInboxIndex, MiruStreamId streamId, int[] indexedIds) throws Exception {
        byte[] primitiveBuffer = new byte[8];
        int expected = miruInboxIndex.getLastActivityIndex(streamId, primitiveBuffer);
        assertEquals(expected, indexedIds[indexedIds.length - 1]);

        int nextId = Integer.MAX_VALUE / 3;
        miruInboxIndex.getAppender(streamId).appendAndExtend(Collections.<Integer>emptyList(), nextId, primitiveBuffer);
        int lastActivityIndex = miruInboxIndex.getLastActivityIndex(streamId, primitiveBuffer);
        assertEquals(lastActivityIndex, expected);
    }

    @DataProvider(name = "miruInboxIndexDataProvider")
    public Object[][] miruInboxIndexDataProvider() throws Exception {
        MiruStreamId miruStreamId = new MiruStreamId(new byte[] { 2 });
        MiruTenantId tenantId = new MiruTenantId(new byte[] { 1 });
        MiruPartitionCoord coord = new MiruPartitionCoord(tenantId, MiruPartitionId.of(0), new MiruHost("localhost", 10000));
        MiruBitmapsRoaringBuffer bitmaps = new MiruBitmapsRoaringBuffer();

        //TODO unnecessary casts, but the wildcards cause some IDE confusion
        MiruContext<ImmutableRoaringBitmap, ?> inMemoryContext = IndexTestUtil.buildInMemoryContext(4, (MiruBitmaps) bitmaps, coord);
        MiruInboxIndex<ImmutableRoaringBitmap> inMemoryIndex = inMemoryContext.inboxIndex;

        MiruContext<ImmutableRoaringBitmap, ?> onDiskContext = IndexTestUtil.buildOnDiskContext(4, (MiruBitmaps) bitmaps, coord);
        MiruInboxIndex<ImmutableRoaringBitmap> onDiskIndex = onDiskContext.inboxIndex;

        return new Object[][] {
            { inMemoryIndex, miruStreamId },
            { onDiskIndex, miruStreamId }
        };
    }

    @DataProvider(name = "miruInboxIndexDataProviderWithData")
    public Object[][] miruInboxIndexDataProviderWithData() throws Exception {
        byte[] primitiveBuffer = new byte[8];
        MiruStreamId streamId = new MiruStreamId(new byte[] { 3 });

        // Create in-memory inbox index
        MiruBitmapsRoaringBuffer bitmaps = new MiruBitmapsRoaringBuffer();
        MiruTenantId tenantId = new MiruTenantId(new byte[] { 1 });
        MiruPartitionCoord coord = new MiruPartitionCoord(tenantId, MiruPartitionId.of(0), new MiruHost("localhost", 10000));

        int[] data = new int[] { 1, 2, 3, 4 };
        int[] data1_2 = new int[] { 1, 2 };
        int[] data2_2 = new int[] { 3, 4 };

        //TODO unnecessary casts, but the wildcards cause some IDE confusion
        MiruContext<ImmutableRoaringBitmap, ?> unmergedInMemoryContext = IndexTestUtil.buildInMemoryContext(4, (MiruBitmaps) bitmaps, coord);
        MiruInboxIndex<ImmutableRoaringBitmap> unmergedInMemoryIndex = unmergedInMemoryContext.inboxIndex;
        unmergedInMemoryIndex.append(streamId, primitiveBuffer, data);

        MiruContext<ImmutableRoaringBitmap, ?> unmergedOnDiskContext = IndexTestUtil.buildOnDiskContext(4, (MiruBitmaps) bitmaps, coord);
        MiruInboxIndex<ImmutableRoaringBitmap> unmergedOnDiskIndex = unmergedOnDiskContext.inboxIndex;
        unmergedOnDiskIndex.append(streamId, primitiveBuffer, data);

        MiruContext<ImmutableRoaringBitmap, ?> mergedInMemoryContext = IndexTestUtil.buildInMemoryContext(4, (MiruBitmaps) bitmaps, coord);
        MiruInboxIndex<ImmutableRoaringBitmap> mergedInMemoryIndex = mergedInMemoryContext.inboxIndex;
        mergedInMemoryIndex.append(streamId, primitiveBuffer, data);
        ((MiruDeltaInboxIndex<MutableRoaringBitmap, ImmutableRoaringBitmap>) mergedInMemoryIndex).merge(primitiveBuffer);

        MiruContext<ImmutableRoaringBitmap, ?> mergedOnDiskContext = IndexTestUtil.buildOnDiskContext(4, (MiruBitmaps) bitmaps, coord);
        MiruInboxIndex<ImmutableRoaringBitmap> mergedOnDiskIndex = mergedOnDiskContext.inboxIndex;
        mergedOnDiskIndex.append(streamId, primitiveBuffer, data);
        ((MiruDeltaInboxIndex<MutableRoaringBitmap, ImmutableRoaringBitmap>) mergedOnDiskIndex).merge(primitiveBuffer);

        MiruContext<ImmutableRoaringBitmap, ?> partiallyMergedInMemoryContext = IndexTestUtil.buildInMemoryContext(4, (MiruBitmaps) bitmaps, coord);
        MiruInboxIndex<ImmutableRoaringBitmap> partiallyMergedInMemoryIndex = partiallyMergedInMemoryContext.inboxIndex;
        partiallyMergedInMemoryIndex.append(streamId, primitiveBuffer, data1_2);
        ((MiruDeltaInboxIndex<MutableRoaringBitmap, ImmutableRoaringBitmap>) partiallyMergedInMemoryIndex).merge(primitiveBuffer);
        partiallyMergedInMemoryIndex.append(streamId, primitiveBuffer, data2_2);

        MiruContext<ImmutableRoaringBitmap, ?> partiallyMergedOnDiskContext = IndexTestUtil.buildOnDiskContext(4, (MiruBitmaps) bitmaps, coord);
        MiruInboxIndex<ImmutableRoaringBitmap> partiallyMergedOnDiskIndex = partiallyMergedOnDiskContext.inboxIndex;
        partiallyMergedOnDiskIndex.append(streamId, primitiveBuffer, data1_2);
        ((MiruDeltaInboxIndex<MutableRoaringBitmap, ImmutableRoaringBitmap>) partiallyMergedOnDiskIndex).merge(primitiveBuffer);
        partiallyMergedOnDiskIndex.append(streamId, primitiveBuffer, data2_2);

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
