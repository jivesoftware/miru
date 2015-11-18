package com.jivesoftware.os.miru.service.index;

import com.google.common.base.Optional;
import com.google.common.collect.Lists;
import com.googlecode.javaewah.EWAHCompressedBitmap;
import com.jivesoftware.os.miru.api.MiruHost;
import com.jivesoftware.os.miru.api.MiruPartitionCoord;
import com.jivesoftware.os.miru.api.activity.MiruPartitionId;
import com.jivesoftware.os.miru.api.base.MiruStreamId;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.bitmaps.ewah.MiruBitmapsEWAH;
import com.jivesoftware.os.miru.plugin.bitmap.MiruBitmaps;
import com.jivesoftware.os.miru.plugin.bitmap.MiruIntIterator;
import com.jivesoftware.os.miru.plugin.index.MiruUnreadTrackingIndex;
import com.jivesoftware.os.miru.service.index.delta.MiruDeltaUnreadTrackingIndex;
import java.util.List;
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
    public void testDefaultData(MiruBitmaps<EWAHCompressedBitmap> bitmaps,
        MiruUnreadTrackingIndex<EWAHCompressedBitmap> miruUnreadTrackingIndex,
        MiruStreamId streamId,
        List<Integer> expected)
        throws Exception {
        Optional<EWAHCompressedBitmap> unread = miruUnreadTrackingIndex.getUnread(streamId).getIndex();
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
    public void testIndex(MiruBitmaps<EWAHCompressedBitmap> bitmaps,
        MiruUnreadTrackingIndex<EWAHCompressedBitmap> miruUnreadTrackingIndex,
        MiruStreamId streamId,
        List<Integer> expected)
        throws Exception {
        Optional<EWAHCompressedBitmap> unreadIndex = miruUnreadTrackingIndex.getUnread(streamId).getIndex();
        assertNotNull(unreadIndex);
        assertFalse(unreadIndex.isPresent());

        miruUnreadTrackingIndex.append(streamId, 1);
        miruUnreadTrackingIndex.append(streamId, 3);
        miruUnreadTrackingIndex.append(streamId, 5);

        unreadIndex = miruUnreadTrackingIndex.getUnread(streamId).getIndex();
        assertNotNull(unreadIndex);
        assertTrue(unreadIndex.isPresent());
        EWAHCompressedBitmap unreadBitmap = unreadIndex.get();
        assertTrue(unreadBitmap.get(1));
        assertFalse(unreadBitmap.get(2));
        assertTrue(unreadBitmap.get(3));
        assertFalse(unreadBitmap.get(4));
        assertTrue(unreadBitmap.get(5));
    }

    @Test(dataProvider = "miruUnreadTrackingIndexDataProviderWithoutData")
    public void testUnread(MiruBitmaps<EWAHCompressedBitmap> bitmaps,
        MiruUnreadTrackingIndex<EWAHCompressedBitmap> miruUnreadTrackingIndex,
        MiruStreamId streamId,
        List<Integer> expected)
        throws Exception {
        Optional<EWAHCompressedBitmap> unread = miruUnreadTrackingIndex.getUnread(streamId).getIndex();
        assertFalse(unread.isPresent());

        miruUnreadTrackingIndex.append(streamId, 1);
        miruUnreadTrackingIndex.append(streamId, 3);

        EWAHCompressedBitmap readMask = new EWAHCompressedBitmap();
        readMask.set(1);
        readMask.set(2);
        miruUnreadTrackingIndex.applyRead(streamId, readMask);

        unread = miruUnreadTrackingIndex.getUnread(streamId).getIndex();
        assertTrue(unread.isPresent());
        assertEquals(unread.get().cardinality(), 1);
        assertTrue(unread.get().get(3));

        EWAHCompressedBitmap unreadMask = new EWAHCompressedBitmap();
        unreadMask.set(1);
        unreadMask.set(3);
        miruUnreadTrackingIndex.applyUnread(streamId, unreadMask);

        unread = miruUnreadTrackingIndex.getUnread(streamId).getIndex();
        assertTrue(unread.isPresent());
        assertEquals(unread.get().cardinality(), 2);
        assertTrue(unread.get().get(1));
        assertTrue(unread.get().get(3));
    }

    @Test(dataProvider = "miruUnreadTrackingIndexDataProviderWithData")
    public void testRead(MiruBitmaps<EWAHCompressedBitmap> bitmaps,
        MiruUnreadTrackingIndex<EWAHCompressedBitmap> miruUnreadTrackingIndex,
        MiruStreamId streamId,
        List<Integer> expected)
        throws Exception {

        assertTrue(expected.size() > 2, "Test requires at least 2 data");

        EWAHCompressedBitmap unread = miruUnreadTrackingIndex.getUnread(streamId).getIndex().get();
        assertEquals(unread.cardinality(), expected.size());

        EWAHCompressedBitmap readMask = new EWAHCompressedBitmap();
        readMask.set(expected.get(0));
        readMask.set(expected.get(1));
        readMask.set(expected.get(expected.size() - 1) + 1);
        miruUnreadTrackingIndex.applyRead(streamId, readMask);

        unread = miruUnreadTrackingIndex.getUnread(streamId).getIndex().get();
        assertEquals(unread.cardinality(), expected.size() - 2);
        for (int i = 2; i < expected.size(); i++) {
            assertTrue(unread.get(expected.get(i)));
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

        MiruBitmapsEWAH bitmaps = new MiruBitmapsEWAH(4);
        MiruPartitionCoord coord = new MiruPartitionCoord(new MiruTenantId("test".getBytes()), MiruPartitionId.of(0), new MiruHost("localhost", 10000));

        MiruUnreadTrackingIndex<EWAHCompressedBitmap> unmergedInMemoryIndex = buildInMemoryContext(4, bitmaps, coord).unreadTrackingIndex;
        unmergedInMemoryIndex.append(streamId, data);

        MiruUnreadTrackingIndex<EWAHCompressedBitmap> unmergedOnDiskIndex = buildOnDiskContext(4, bitmaps, coord).unreadTrackingIndex;
        unmergedOnDiskIndex.append(streamId, data);

        MiruUnreadTrackingIndex<EWAHCompressedBitmap> mergedInMemoryIndex = buildInMemoryContext(4, bitmaps, coord).unreadTrackingIndex;
        mergedInMemoryIndex.append(streamId, data);
        ((MiruDeltaUnreadTrackingIndex<EWAHCompressedBitmap>) mergedInMemoryIndex).merge();

        MiruUnreadTrackingIndex<EWAHCompressedBitmap> mergedOnDiskIndex = buildOnDiskContext(4, bitmaps, coord).unreadTrackingIndex;
        mergedOnDiskIndex.append(streamId, data);
        ((MiruDeltaUnreadTrackingIndex<EWAHCompressedBitmap>) mergedOnDiskIndex).merge();

        MiruUnreadTrackingIndex<EWAHCompressedBitmap> partiallyMergedInMemoryIndex = buildInMemoryContext(4, bitmaps, coord).unreadTrackingIndex;
        partiallyMergedInMemoryIndex.append(streamId, data1_2);
        ((MiruDeltaUnreadTrackingIndex<EWAHCompressedBitmap>) partiallyMergedInMemoryIndex).merge();
        partiallyMergedInMemoryIndex.append(streamId, data2_2);

        MiruUnreadTrackingIndex<EWAHCompressedBitmap> partiallyMergedOnDiskIndex = buildOnDiskContext(4, bitmaps, coord).unreadTrackingIndex;
        partiallyMergedOnDiskIndex.append(streamId, data1_2);
        ((MiruDeltaUnreadTrackingIndex<EWAHCompressedBitmap>) partiallyMergedOnDiskIndex).merge();
        partiallyMergedOnDiskIndex.append(streamId, data2_2);

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
