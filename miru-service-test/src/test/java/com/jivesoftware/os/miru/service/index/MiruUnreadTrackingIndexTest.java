package com.jivesoftware.os.miru.service.index;

import com.google.common.base.Optional;
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
import com.jivesoftware.os.miru.plugin.index.MiruUnreadTrackingIndex;
import java.util.List;
import org.apache.commons.lang3.ArrayUtils;
import org.roaringbitmap.RoaringBitmap;
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
    public void testDefaultData(MiruBitmaps<RoaringBitmap, RoaringBitmap> bitmaps,
        MiruUnreadTrackingIndex<RoaringBitmap, RoaringBitmap> miruUnreadTrackingIndex,
        MiruStreamId streamId,
        List<Integer> expected)
        throws Exception {
        StackBuffer stackBuffer = new StackBuffer();
        Optional<RoaringBitmap> unread = miruUnreadTrackingIndex.getUnread(streamId).getIndex(stackBuffer);
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
    public void testIndex(MiruBitmaps<RoaringBitmap, RoaringBitmap> bitmaps,
        MiruUnreadTrackingIndex<RoaringBitmap, RoaringBitmap> miruUnreadTrackingIndex,
        MiruStreamId streamId,
        List<Integer> expected)
        throws Exception {
        StackBuffer stackBuffer = new StackBuffer();
        Optional<RoaringBitmap> unreadIndex = miruUnreadTrackingIndex.getUnread(streamId).getIndex(stackBuffer);
        assertNotNull(unreadIndex);
        assertFalse(unreadIndex.isPresent());

        miruUnreadTrackingIndex.append(streamId, stackBuffer, 1);
        miruUnreadTrackingIndex.append(streamId, stackBuffer, 3);
        miruUnreadTrackingIndex.append(streamId, stackBuffer, 5);

        unreadIndex = miruUnreadTrackingIndex.getUnread(streamId).getIndex(stackBuffer);
        assertNotNull(unreadIndex);
        assertTrue(unreadIndex.isPresent());
        RoaringBitmap unreadBitmap = unreadIndex.get();
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
        Optional<RoaringBitmap> unread = miruUnreadTrackingIndex.getUnread(streamId).getIndex(stackBuffer);
        assertFalse(unread.isPresent());

        miruUnreadTrackingIndex.append(streamId, stackBuffer, 1);
        miruUnreadTrackingIndex.append(streamId, stackBuffer, 3);

        RoaringBitmap readMask = new RoaringBitmap();
        readMask.add(1);
        readMask.add(2);
        miruUnreadTrackingIndex.applyRead(streamId, readMask, stackBuffer);

        unread = miruUnreadTrackingIndex.getUnread(streamId).getIndex(stackBuffer);
        assertTrue(unread.isPresent());
        assertEquals(unread.get().getCardinality(), 1);
        assertTrue(unread.get().contains(3));

        RoaringBitmap unreadMask = new RoaringBitmap();
        unreadMask.add(1);
        unreadMask.add(3);
        miruUnreadTrackingIndex.applyUnread(streamId, unreadMask, stackBuffer);

        unread = miruUnreadTrackingIndex.getUnread(streamId).getIndex(stackBuffer);
        assertTrue(unread.isPresent());
        assertEquals(unread.get().getCardinality(), 2);
        assertTrue(unread.get().contains(1));
        assertTrue(unread.get().contains(3));
    }

    @Test(dataProvider = "miruUnreadTrackingIndexDataProviderWithData")
    public void testRead(MiruBitmaps<RoaringBitmap, RoaringBitmap> bitmaps,
        MiruUnreadTrackingIndex<RoaringBitmap, RoaringBitmap> miruUnreadTrackingIndex,
        MiruStreamId streamId,
        List<Integer> expected)
        throws Exception {
        StackBuffer stackBuffer = new StackBuffer();
        assertTrue(expected.size() > 2, "Test requires at least 2 data");

        RoaringBitmap unread = miruUnreadTrackingIndex.getUnread(streamId).getIndex(stackBuffer).get();
        assertEquals(unread.getCardinality(), expected.size());

        RoaringBitmap readMask = new RoaringBitmap();
        readMask.add(expected.get(0));
        readMask.add(expected.get(1));
        readMask.add(expected.get(expected.size() - 1) + 1);
        miruUnreadTrackingIndex.applyRead(streamId, readMask, stackBuffer);

        unread = miruUnreadTrackingIndex.getUnread(streamId).getIndex(stackBuffer).get();
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
