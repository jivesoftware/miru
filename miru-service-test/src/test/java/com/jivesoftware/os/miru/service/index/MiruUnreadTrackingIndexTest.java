package com.jivesoftware.os.miru.service.index;

import com.google.common.base.Optional;
import com.google.common.collect.Lists;
import com.jivesoftware.os.miru.api.MiruHost;
import com.jivesoftware.os.miru.api.MiruPartitionCoord;
import com.jivesoftware.os.miru.api.activity.MiruPartitionId;
import com.jivesoftware.os.miru.api.base.MiruStreamId;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.bitmaps.roaring5.buffer.MiruBitmapsRoaringBuffer;
import com.jivesoftware.os.miru.plugin.bitmap.MiruBitmaps;
import com.jivesoftware.os.miru.plugin.bitmap.MiruIntIterator;
import com.jivesoftware.os.miru.plugin.index.MiruUnreadTrackingIndex;
import com.jivesoftware.os.miru.service.index.delta.MiruDeltaUnreadTrackingIndex;
import java.util.List;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;
import org.roaringbitmap.buffer.MutableRoaringBitmap;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static com.jivesoftware.os.miru.service.IndexTestUtil.buildInMemoryContext;
import static com.jivesoftware.os.miru.service.IndexTestUtil.buildOnDiskContext;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

public class MiruUnreadTrackingIndexTest {

    @Test(dataProvider = "miruUnreadTrackingIndexDataProviderWithData")
    public void testDefaultData(MiruBitmaps<MutableRoaringBitmap, ImmutableRoaringBitmap> bitmaps,
        MiruUnreadTrackingIndex<ImmutableRoaringBitmap> miruUnreadTrackingIndex,
        MiruStreamId streamId,
        List<Integer> expected)
        throws Exception {
        byte[] primitiveBuffer = new byte[8];
        Optional<ImmutableRoaringBitmap> unread = miruUnreadTrackingIndex.getUnread(streamId).getIndex(primitiveBuffer);
        assertNotNull(unread);
        assertTrue(unread.isPresent());

        List<Integer> actual = Lists.newArrayList();
        MiruIntIterator iter = bitmaps.intIterator(unread.get());
        while (iter.hasNext()) {
            actual.add(iter.next());
        }
        assertEquals(actual, expected);
    }

    @Test(dataProvider = "miruUnreadTrackingIndexDataProviderWithoutData")
    public void testIndex(MiruBitmaps<MutableRoaringBitmap, ImmutableRoaringBitmap> bitmaps,
        MiruUnreadTrackingIndex<ImmutableRoaringBitmap> miruUnreadTrackingIndex,
        MiruStreamId streamId,
        List<Integer> expected)
        throws Exception {
        byte[] primitiveBuffer = new byte[8];
        Optional<ImmutableRoaringBitmap> unreadIndex = miruUnreadTrackingIndex.getUnread(streamId).getIndex(primitiveBuffer);
        assertNotNull(unreadIndex);
        assertFalse(unreadIndex.isPresent());

        miruUnreadTrackingIndex.append(streamId, primitiveBuffer, 1);
        miruUnreadTrackingIndex.append(streamId, primitiveBuffer, 3);
        miruUnreadTrackingIndex.append(streamId, primitiveBuffer, 5);

        unreadIndex = miruUnreadTrackingIndex.getUnread(streamId).getIndex(primitiveBuffer);
        assertNotNull(unreadIndex);
        assertTrue(unreadIndex.isPresent());
        ImmutableRoaringBitmap unreadBitmap = unreadIndex.get();
        assertTrue(unreadBitmap.contains(1));
        assertFalse(unreadBitmap.contains(2));
        assertTrue(unreadBitmap.contains(3));
        assertFalse(unreadBitmap.contains(4));
        assertTrue(unreadBitmap.contains(5));
    }

    @Test(dataProvider = "miruUnreadTrackingIndexDataProviderWithoutData")
    public void testUnread(MiruBitmaps<MutableRoaringBitmap, ImmutableRoaringBitmap> bitmaps,
        MiruUnreadTrackingIndex<ImmutableRoaringBitmap> miruUnreadTrackingIndex,
        MiruStreamId streamId,
        List<Integer> expected)
        throws Exception {
        byte[] primitiveBuffer = new byte[8];
        Optional<ImmutableRoaringBitmap> unread = miruUnreadTrackingIndex.getUnread(streamId).getIndex(primitiveBuffer);
        assertFalse(unread.isPresent());

        miruUnreadTrackingIndex.append(streamId, primitiveBuffer, 1);
        miruUnreadTrackingIndex.append(streamId, primitiveBuffer, 3);

        MutableRoaringBitmap readMask = new MutableRoaringBitmap();
        readMask.add(1);
        readMask.add(2);
        miruUnreadTrackingIndex.applyRead(streamId, readMask, primitiveBuffer);

        unread = miruUnreadTrackingIndex.getUnread(streamId).getIndex(primitiveBuffer);
        assertTrue(unread.isPresent());
        assertEquals(unread.get().getCardinality(), 1);
        assertTrue(unread.get().contains(3));

        MutableRoaringBitmap unreadMask = new MutableRoaringBitmap();
        unreadMask.add(1);
        unreadMask.add(3);
        miruUnreadTrackingIndex.applyUnread(streamId, unreadMask, primitiveBuffer);

        unread = miruUnreadTrackingIndex.getUnread(streamId).getIndex(primitiveBuffer);
        assertTrue(unread.isPresent());
        assertEquals(unread.get().getCardinality(), 2);
        assertTrue(unread.get().contains(1));
        assertTrue(unread.get().contains(3));
    }

    @Test(dataProvider = "miruUnreadTrackingIndexDataProviderWithData")
    public void testRead(MiruBitmaps<MutableRoaringBitmap, ImmutableRoaringBitmap> bitmaps,
        MiruUnreadTrackingIndex<ImmutableRoaringBitmap> miruUnreadTrackingIndex,
        MiruStreamId streamId,
        List<Integer> expected)
        throws Exception {
        byte[] primitiveBuffer = new byte[8];
        assertTrue(expected.size() > 2, "Test requires at least 2 data");

        ImmutableRoaringBitmap unread = miruUnreadTrackingIndex.getUnread(streamId).getIndex(primitiveBuffer).get();
        assertEquals(unread.getCardinality(), expected.size());

        MutableRoaringBitmap readMask = new MutableRoaringBitmap();
        readMask.add(expected.get(0));
        readMask.add(expected.get(1));
        readMask.add(expected.get(expected.size() - 1) + 1);
        miruUnreadTrackingIndex.applyRead(streamId, readMask, primitiveBuffer);

        unread = miruUnreadTrackingIndex.getUnread(streamId).getIndex(primitiveBuffer).get();
        assertEquals(unread.getCardinality(), expected.size() - 2);
        for (int i = 2; i < expected.size(); i++) {
            assertTrue(unread.contains(expected.get(i)));
        }
    }

    @DataProvider(name = "miruUnreadTrackingIndexDataProviderWithoutData")
    public Object[][] miruUnreadTrackingIndexDataProviderWithoutData() throws Exception {
        return generateUnreadIndexes(new MiruTenantId(new byte[] { 1 }), new int[0]);
    }

    @DataProvider(name = "miruUnreadTrackingIndexDataProviderWithData")
    public Object[][] miruUnreadTrackingIndexDataProviderWithData() throws Exception {
        return generateUnreadIndexes(new MiruTenantId(new byte[] { 1 }), new int[] { 1, 2, 3, 4 });
    }

    private Object[][] generateUnreadIndexes(MiruTenantId tenantId, int[] data) throws Exception {
        byte[] primitiveBuffer = new byte[8];
        final MiruStreamId streamId = new MiruStreamId(new byte[] { 2 });

        assertTrue(data.length % 2 == 0, "Need an even number of data");

        List<Integer> expected = Lists.newArrayList();
        for (int d : data) {
            expected.add(d);
        }

        int[] data1_2 = new int[data.length / 2];
        int[] data2_2 = new int[data.length / 2];
        System.arraycopy(data, 0, data1_2, 0, data.length / 2);
        System.arraycopy(data, data.length / 2, data2_2, 0, data.length / 2);

        MiruBitmapsRoaringBuffer bitmaps = new MiruBitmapsRoaringBuffer();
        MiruPartitionCoord coord = new MiruPartitionCoord(new MiruTenantId("test".getBytes()), MiruPartitionId.of(0), new MiruHost("localhost", 10000));

        MiruUnreadTrackingIndex<ImmutableRoaringBitmap> unmergedInMemoryIndex = buildInMemoryContext(4, bitmaps, coord).unreadTrackingIndex;
        unmergedInMemoryIndex.append(streamId, primitiveBuffer, data);

        MiruUnreadTrackingIndex<ImmutableRoaringBitmap> unmergedOnDiskIndex = buildOnDiskContext(4, bitmaps, coord).unreadTrackingIndex;
        unmergedOnDiskIndex.append(streamId, primitiveBuffer, data);

        MiruUnreadTrackingIndex<ImmutableRoaringBitmap> mergedInMemoryIndex = buildInMemoryContext(4, bitmaps, coord).unreadTrackingIndex;
        mergedInMemoryIndex.append(streamId, primitiveBuffer, data);
        ((MiruDeltaUnreadTrackingIndex<MutableRoaringBitmap, ImmutableRoaringBitmap>) mergedInMemoryIndex).merge(primitiveBuffer);

        MiruUnreadTrackingIndex<ImmutableRoaringBitmap> mergedOnDiskIndex = buildOnDiskContext(4, bitmaps, coord).unreadTrackingIndex;
        mergedOnDiskIndex.append(streamId, primitiveBuffer, data);
        ((MiruDeltaUnreadTrackingIndex<ImmutableRoaringBitmap, ImmutableRoaringBitmap>) mergedOnDiskIndex).merge(primitiveBuffer);

        MiruUnreadTrackingIndex<ImmutableRoaringBitmap> partiallyMergedInMemoryIndex = buildInMemoryContext(4, bitmaps, coord).unreadTrackingIndex;
        partiallyMergedInMemoryIndex.append(streamId, primitiveBuffer, data1_2);
        ((MiruDeltaUnreadTrackingIndex<MutableRoaringBitmap, ImmutableRoaringBitmap>) partiallyMergedInMemoryIndex).merge(primitiveBuffer);
        partiallyMergedInMemoryIndex.append(streamId, primitiveBuffer, data2_2);

        MiruUnreadTrackingIndex<ImmutableRoaringBitmap> partiallyMergedOnDiskIndex = buildOnDiskContext(4, bitmaps, coord).unreadTrackingIndex;
        partiallyMergedOnDiskIndex.append(streamId, primitiveBuffer, data1_2);
        ((MiruDeltaUnreadTrackingIndex<MutableRoaringBitmap, ImmutableRoaringBitmap>) partiallyMergedOnDiskIndex).merge(primitiveBuffer);
        partiallyMergedOnDiskIndex.append(streamId, primitiveBuffer, data2_2);

        return new Object[][] {
            { bitmaps, unmergedInMemoryIndex, streamId, expected },
            { bitmaps, unmergedOnDiskIndex, streamId, expected },
            { bitmaps, mergedInMemoryIndex, streamId, expected },
            { bitmaps, mergedOnDiskIndex, streamId, expected },
            { bitmaps, partiallyMergedInMemoryIndex, streamId, expected },
            { bitmaps, partiallyMergedOnDiskIndex, streamId, expected }
        };
    }
}
