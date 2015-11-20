package com.jivesoftware.os.miru.service.index;

import com.google.common.base.Optional;
import com.googlecode.javaewah.EWAHCompressedBitmap;
import com.jivesoftware.os.miru.api.MiruHost;
import com.jivesoftware.os.miru.api.MiruPartitionCoord;
import com.jivesoftware.os.miru.api.activity.MiruPartitionId;
import com.jivesoftware.os.miru.api.base.MiruStreamId;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.bitmaps.ewah.MiruBitmapsEWAH;
import com.jivesoftware.os.miru.plugin.bitmap.MiruBitmaps;
import com.jivesoftware.os.miru.plugin.index.MiruInboxIndex;
import com.jivesoftware.os.miru.plugin.index.MiruInvertedIndexAppender;
import com.jivesoftware.os.miru.service.IndexTestUtil;
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
        byte[] primitiveBuffer = new byte[8];
        Optional<EWAHCompressedBitmap> inbox = miruInboxIndex.getInbox(miruStreamId).getIndex(primitiveBuffer);
        assertNotNull(inbox);
        assertFalse(inbox.isPresent());
    }

    @Test(dataProvider = "miruInboxIndexDataProvider")
    public void testGetEmptyInboxAndCreate(MiruInboxIndex<EWAHCompressedBitmap> miruInboxIndex, MiruStreamId miruStreamId)
        throws Exception {
        byte[] primitiveBuffer = new byte[8];
        MiruInvertedIndexAppender inbox = miruInboxIndex.getAppender(miruStreamId);
        assertNotNull(inbox);
        Optional<EWAHCompressedBitmap> inboxIndex = miruInboxIndex.getInbox(miruStreamId).getIndex(primitiveBuffer);
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
    public void testIndexedData(MiruInboxIndex<EWAHCompressedBitmap> miruInboxIndex, MiruStreamId streamId, int[] indexedIds) throws Exception {
        byte[] primitiveBuffer = new byte[8];
        Optional<EWAHCompressedBitmap> inbox = miruInboxIndex.getInbox(streamId).getIndex(primitiveBuffer);
        assertNotNull(inbox);
        assertTrue(inbox.isPresent());
        for (int indexedId : indexedIds) {
            assertTrue(inbox.get().get(indexedId));
        }
    }

    @Test(dataProvider = "miruInboxIndexDataProviderWithData")
    public void testDefaultLastActivityIndexWithNewStreamId(MiruInboxIndex<EWAHCompressedBitmap> miruInboxIndex, MiruStreamId streamId,
        int[] indexedIds) throws Exception {
        byte[] primitiveBuffer = new byte[8];
        MiruStreamId newStreamId = new MiruStreamId(new byte[]{1});
        int lastActivityIndex = miruInboxIndex.getLastActivityIndex(newStreamId, primitiveBuffer);
        assertEquals(lastActivityIndex, -1);

        int nextId = Integer.MAX_VALUE / 2;
        miruInboxIndex.append(newStreamId, primitiveBuffer, nextId);
        lastActivityIndex = miruInboxIndex.getLastActivityIndex(newStreamId, primitiveBuffer);
        assertEquals(lastActivityIndex, nextId);
    }

    @Test(dataProvider = "miruInboxIndexDataProviderWithData")
    public void testLastActivityIndex(MiruInboxIndex<EWAHCompressedBitmap> miruInboxIndex, MiruStreamId streamId, int[] indexedIds) throws Exception {
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
        MiruStreamId miruStreamId = new MiruStreamId(new byte[]{2});
        MiruTenantId tenantId = new MiruTenantId(new byte[]{1});
        MiruPartitionCoord coord = new MiruPartitionCoord(tenantId, MiruPartitionId.of(0), new MiruHost("localhost", 10000));
        MiruBitmapsEWAH bitmaps = new MiruBitmapsEWAH(10);

        //TODO unnecessary casts, but the wildcards cause some IDE confusion
        MiruContext<EWAHCompressedBitmap, ?> inMemoryContext = IndexTestUtil.buildInMemoryContext(4, (MiruBitmaps) bitmaps, coord);
        MiruInboxIndex<EWAHCompressedBitmap> inMemoryIndex = inMemoryContext.inboxIndex;

        MiruContext<EWAHCompressedBitmap, ?> onDiskContext = IndexTestUtil.buildOnDiskContext(4, (MiruBitmaps) bitmaps, coord);
        MiruInboxIndex<EWAHCompressedBitmap> onDiskIndex = onDiskContext.inboxIndex;

        return new Object[][]{
            {inMemoryIndex, miruStreamId},
            {onDiskIndex, miruStreamId}
        };
    }

    @DataProvider(name = "miruInboxIndexDataProviderWithData")
    public Object[][] miruInboxIndexDataProviderWithData() throws Exception {
        byte[] primitiveBuffer = new byte[8];
        MiruStreamId streamId = new MiruStreamId(new byte[]{3});

        // Create in-memory inbox index
        MiruBitmapsEWAH bitmaps = new MiruBitmapsEWAH(10);
        MiruTenantId tenantId = new MiruTenantId(new byte[]{1});
        MiruPartitionCoord coord = new MiruPartitionCoord(tenantId, MiruPartitionId.of(0), new MiruHost("localhost", 10000));

        int[] data = new int[]{1, 2, 3, 4};
        int[] data1_2 = new int[]{1, 2};
        int[] data2_2 = new int[]{3, 4};

        //TODO unnecessary casts, but the wildcards cause some IDE confusion
        MiruContext<EWAHCompressedBitmap, ?> unmergedInMemoryContext = IndexTestUtil.buildInMemoryContext(4, (MiruBitmaps) bitmaps, coord);
        MiruInboxIndex<EWAHCompressedBitmap> unmergedInMemoryIndex = unmergedInMemoryContext.inboxIndex;
        unmergedInMemoryIndex.append(streamId, primitiveBuffer, data);

        MiruContext<EWAHCompressedBitmap, ?> unmergedOnDiskContext = IndexTestUtil.buildOnDiskContext(4, (MiruBitmaps) bitmaps, coord);
        MiruInboxIndex<EWAHCompressedBitmap> unmergedOnDiskIndex = unmergedOnDiskContext.inboxIndex;
        unmergedOnDiskIndex.append(streamId, primitiveBuffer, data);

        MiruContext<EWAHCompressedBitmap, ?> mergedInMemoryContext = IndexTestUtil.buildInMemoryContext(4, (MiruBitmaps) bitmaps, coord);
        MiruInboxIndex<EWAHCompressedBitmap> mergedInMemoryIndex = mergedInMemoryContext.inboxIndex;
        mergedInMemoryIndex.append(streamId, primitiveBuffer, data);
        ((MiruDeltaInboxIndex<EWAHCompressedBitmap>) mergedInMemoryIndex).merge(primitiveBuffer);

        MiruContext<EWAHCompressedBitmap, ?> mergedOnDiskContext = IndexTestUtil.buildOnDiskContext(4, (MiruBitmaps) bitmaps, coord);
        MiruInboxIndex<EWAHCompressedBitmap> mergedOnDiskIndex = mergedOnDiskContext.inboxIndex;
        mergedOnDiskIndex.append(streamId, primitiveBuffer, data);
        ((MiruDeltaInboxIndex<EWAHCompressedBitmap>) mergedOnDiskIndex).merge(primitiveBuffer);

        MiruContext<EWAHCompressedBitmap, ?> partiallyMergedInMemoryContext = IndexTestUtil.buildInMemoryContext(4, (MiruBitmaps) bitmaps, coord);
        MiruInboxIndex<EWAHCompressedBitmap> partiallyMergedInMemoryIndex = partiallyMergedInMemoryContext.inboxIndex;
        partiallyMergedInMemoryIndex.append(streamId, primitiveBuffer, data1_2);
        ((MiruDeltaInboxIndex<EWAHCompressedBitmap>) partiallyMergedInMemoryIndex).merge(primitiveBuffer);
        partiallyMergedInMemoryIndex.append(streamId, primitiveBuffer, data2_2);

        MiruContext<EWAHCompressedBitmap, ?> partiallyMergedOnDiskContext = IndexTestUtil.buildOnDiskContext(4, (MiruBitmaps) bitmaps, coord);
        MiruInboxIndex<EWAHCompressedBitmap> partiallyMergedOnDiskIndex = partiallyMergedOnDiskContext.inboxIndex;
        partiallyMergedOnDiskIndex.append(streamId, primitiveBuffer, data1_2);
        ((MiruDeltaInboxIndex<EWAHCompressedBitmap>) partiallyMergedOnDiskIndex).merge(primitiveBuffer);
        partiallyMergedOnDiskIndex.append(streamId, primitiveBuffer, data2_2);

        return new Object[][]{
            {unmergedInMemoryIndex, streamId, data},
            {unmergedOnDiskIndex, streamId, data},
            {mergedInMemoryIndex, streamId, data},
            {mergedOnDiskIndex, streamId, data},
            {partiallyMergedInMemoryIndex, streamId, data},
            {partiallyMergedOnDiskIndex, streamId, data}
        };
    }
}
