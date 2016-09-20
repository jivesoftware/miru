package com.jivesoftware.os.miru.service.index;

import com.google.common.collect.Lists;
import com.jivesoftware.os.filer.io.api.StackBuffer;
import com.jivesoftware.os.miru.api.MiruHost;
import com.jivesoftware.os.miru.api.MiruPartitionCoord;
import com.jivesoftware.os.miru.api.activity.MiruPartitionId;
import com.jivesoftware.os.miru.api.base.MiruStreamId;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.bitmaps.roaring5.MiruBitmapsRoaring;
import com.jivesoftware.os.miru.plugin.bitmap.MiruBitmaps;
import com.jivesoftware.os.miru.plugin.bitmap.MiruIntIterator;
import com.jivesoftware.os.miru.plugin.index.BitmapAndLastId;
import com.jivesoftware.os.miru.plugin.index.MiruUnreadTrackingIndex;
import java.util.List;
import org.apache.commons.lang3.ArrayUtils;
import org.roaringbitmap.RoaringBitmap;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static com.jivesoftware.os.miru.service.IndexTestUtil.buildInMemoryContext;
import static com.jivesoftware.os.miru.service.IndexTestUtil.buildOnDiskContext;
import static com.jivesoftware.os.miru.service.IndexTestUtil.getIndex;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

public class MiruUnreadTrackingIndexTest {

    @Test(dataProvider = "miruUnreadTrackingIndexDataProviderWithData")
    public void testDefaultData(MiruBitmaps<RoaringBitmap, RoaringBitmap> bitmaps,
        MiruUnreadTrackingIndex<RoaringBitmap, RoaringBitmap> miruUnreadTrackingIndex,
        MiruStreamId streamId,
        List<Integer> expected)
        throws Exception {
        StackBuffer stackBuffer = new StackBuffer();
        BitmapAndLastId<RoaringBitmap> unread = getIndex(miruUnreadTrackingIndex.getUnread(streamId), stackBuffer);
        assertNotNull(unread);
        assertTrue(unread.isSet());

        List<Integer> actual = Lists.newArrayList();
        MiruIntIterator iter = bitmaps.intIterator(unread.getBitmap());
        while (iter.hasNext()) {
            actual.add(iter.next());
        }
        assertEquals(actual, expected);
    }

    @Test(dataProvider = "miruUnreadTrackingIndexDataProviderWithoutData")
    public void testIndex(MiruBitmaps<RoaringBitmap, RoaringBitmap> bitmaps,
        MiruUnreadTrackingIndex<RoaringBitmap, RoaringBitmap> miruUnreadTrackingIndex,
        MiruStreamId streamId,
        List<Integer> expected)
        throws Exception {
        StackBuffer stackBuffer = new StackBuffer();
        BitmapAndLastId<RoaringBitmap> unreadIndex = getIndex(miruUnreadTrackingIndex.getUnread(streamId), stackBuffer);
        assertNotNull(unreadIndex);
        assertFalse(unreadIndex.isSet());

        miruUnreadTrackingIndex.append(streamId, stackBuffer, 1);
        miruUnreadTrackingIndex.append(streamId, stackBuffer, 3);
        miruUnreadTrackingIndex.append(streamId, stackBuffer, 5);

        unreadIndex = getIndex(miruUnreadTrackingIndex.getUnread(streamId), stackBuffer);
        assertNotNull(unreadIndex);
        assertTrue(unreadIndex.isSet());
        RoaringBitmap unreadBitmap = unreadIndex.getBitmap();
        assertTrue(unreadBitmap.contains(1));
        assertFalse(unreadBitmap.contains(2));
        assertTrue(unreadBitmap.contains(3));
        assertFalse(unreadBitmap.contains(4));
        assertTrue(unreadBitmap.contains(5));
    }

    @Test(dataProvider = "miruUnreadTrackingIndexDataProviderWithoutData")
    public void testUnread(MiruBitmaps<RoaringBitmap, RoaringBitmap> bitmaps,
        MiruUnreadTrackingIndex<RoaringBitmap, RoaringBitmap> miruUnreadTrackingIndex,
        MiruStreamId streamId,
        List<Integer> expected)
        throws Exception {
        StackBuffer stackBuffer = new StackBuffer();
        BitmapAndLastId<RoaringBitmap> unread = getIndex(miruUnreadTrackingIndex.getUnread(streamId), stackBuffer);
        assertFalse(unread.isSet());

        miruUnreadTrackingIndex.append(streamId, stackBuffer, 1);
        miruUnreadTrackingIndex.append(streamId, stackBuffer, 3);

        RoaringBitmap readMask = new RoaringBitmap();
        readMask.add(1);
        readMask.add(2);
        miruUnreadTrackingIndex.applyRead(streamId, readMask, stackBuffer);

        unread = getIndex(miruUnreadTrackingIndex.getUnread(streamId), stackBuffer);
        assertTrue(unread.isSet());
        assertEquals(unread.getBitmap().getCardinality(), 1);
        assertTrue(unread.getBitmap().contains(3));

        RoaringBitmap unreadMask = new RoaringBitmap();
        unreadMask.add(1);
        unreadMask.add(3);
        miruUnreadTrackingIndex.applyUnread(streamId, unreadMask, stackBuffer);

        unread = getIndex(miruUnreadTrackingIndex.getUnread(streamId), stackBuffer);
        assertTrue(unread.isSet());
        assertEquals(unread.getBitmap().getCardinality(), 2);
        assertTrue(unread.getBitmap().contains(1));
        assertTrue(unread.getBitmap().contains(3));
    }

    @Test(dataProvider = "miruUnreadTrackingIndexDataProviderWithData")
    public void testRead(MiruBitmaps<RoaringBitmap, RoaringBitmap> bitmaps,
        MiruUnreadTrackingIndex<RoaringBitmap, RoaringBitmap> miruUnreadTrackingIndex,
        MiruStreamId streamId,
        List<Integer> expected)
        throws Exception {
        StackBuffer stackBuffer = new StackBuffer();
        assertTrue(expected.size() > 2, "Test requires at least 2 data");

        RoaringBitmap unread = getIndex(miruUnreadTrackingIndex.getUnread(streamId), stackBuffer).getBitmap();
        assertEquals(unread.getCardinality(), expected.size());

        RoaringBitmap readMask = new RoaringBitmap();
        readMask.add(expected.get(0));
        readMask.add(expected.get(1));
        readMask.add(expected.get(expected.size() - 1) + 1);
        miruUnreadTrackingIndex.applyRead(streamId, readMask, stackBuffer);

        unread = getIndex(miruUnreadTrackingIndex.getUnread(streamId), stackBuffer).getBitmap();
        assertEquals(unread.getCardinality(), expected.size() - 2);
        for (int i = 2; i < expected.size(); i++) {
            assertTrue(unread.contains(expected.get(i)));
        }
    }

    @DataProvider(name = "miruUnreadTrackingIndexDataProviderWithoutData")
    public Object[][] miruUnreadTrackingIndexDataProviderWithoutData() throws Exception {
        return ArrayUtils.addAll(generateUnreadIndexes(new MiruTenantId(new byte[] { 1 }), new int[0], false),
            generateUnreadIndexes(new MiruTenantId(new byte[] { 1 }), new int[0], true));
    }

    @DataProvider(name = "miruUnreadTrackingIndexDataProviderWithData")
    public Object[][] miruUnreadTrackingIndexDataProviderWithData() throws Exception {
        return ArrayUtils.addAll(generateUnreadIndexes(new MiruTenantId(new byte[] { 1 }), new int[] { 1, 2, 3, 4 }, false),
            generateUnreadIndexes(new MiruTenantId(new byte[] { 1 }), new int[] { 1, 2, 3, 4 }, true));
    }

    private Object[][] generateUnreadIndexes(MiruTenantId tenantId, int[] data, boolean useLabIndexes) throws Exception {
        StackBuffer stackBuffer = new StackBuffer();
        //MiruSchema schema = new Builder("test", 1).build();
        final MiruStreamId streamId = new MiruStreamId(new byte[] { 2 });

        assertTrue(data.length % 2 == 0, "Need an even number of data");

        List<Integer> expected = Lists.newArrayList();
        for (int d : data) {
            expected.add(d);
        }

        MiruBitmapsRoaring bitmaps = new MiruBitmapsRoaring();
        MiruPartitionCoord coord = new MiruPartitionCoord(new MiruTenantId("test".getBytes()), MiruPartitionId.of(0), new MiruHost("logicalName"));

        MiruUnreadTrackingIndex<RoaringBitmap, RoaringBitmap> unmergedInMemoryIndex = buildInMemoryContext(4, useLabIndexes, true, bitmaps,
            coord).unreadTrackingIndex;
        unmergedInMemoryIndex.append(streamId, stackBuffer, data);

        MiruUnreadTrackingIndex<RoaringBitmap, RoaringBitmap> unmergedOnDiskIndex = buildOnDiskContext(4, useLabIndexes, true, bitmaps,
            coord).unreadTrackingIndex;
        unmergedOnDiskIndex.append(streamId, stackBuffer, data);

        return new Object[][] {
            { bitmaps, unmergedInMemoryIndex, streamId, expected },
            { bitmaps, unmergedOnDiskIndex, streamId, expected }
        };
    }
}
