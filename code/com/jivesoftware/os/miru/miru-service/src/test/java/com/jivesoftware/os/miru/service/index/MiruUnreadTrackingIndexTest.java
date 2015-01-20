package com.jivesoftware.os.miru.service.index;

import com.google.common.base.Optional;
import com.google.common.collect.Lists;
import com.googlecode.javaewah.EWAHCompressedBitmap;
import com.jivesoftware.os.jive.utils.id.Id;
import com.jivesoftware.os.miru.api.MiruHost;
import com.jivesoftware.os.miru.api.MiruPartitionCoord;
import com.jivesoftware.os.miru.api.activity.MiruPartitionId;
import com.jivesoftware.os.miru.api.base.MiruStreamId;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.plugin.bitmap.MiruBitmaps;
import com.jivesoftware.os.miru.plugin.bitmap.MiruIntIterator;
import com.jivesoftware.os.miru.plugin.index.MiruUnreadTrackingIndex;
import com.jivesoftware.os.miru.service.bitmap.MiruBitmapsEWAH;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static com.jivesoftware.os.miru.service.IndexTestUtil.buildHybridContext;
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
        Optional<EWAHCompressedBitmap> unread = miruUnreadTrackingIndex.getUnread(streamId);
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
        Optional<EWAHCompressedBitmap> unreadIndex = miruUnreadTrackingIndex.getUnread(streamId);
        assertNotNull(unreadIndex);
        assertFalse(unreadIndex.isPresent());

        miruUnreadTrackingIndex.index(streamId, 1);
        miruUnreadTrackingIndex.index(streamId, 3);
        miruUnreadTrackingIndex.index(streamId, 5);

        unreadIndex = miruUnreadTrackingIndex.getUnread(streamId);
        assertNotNull(unreadIndex);
        assertTrue(unreadIndex.isPresent());
        EWAHCompressedBitmap unreadBitmap = unreadIndex.get();
        assertTrue(unreadBitmap.get(1));
        assertFalse(unreadBitmap.get(2));
        assertTrue(unreadBitmap.get(3));
        assertFalse(unreadBitmap.get(4));
        assertTrue(unreadBitmap.get(5));
    }

    @Test(dataProvider = "miruUnreadTrackingIndexDataProvider")
    public void testUnread(MiruBitmaps<EWAHCompressedBitmap> bitmaps,
        MiruUnreadTrackingIndex<EWAHCompressedBitmap> miruUnreadTrackingIndex,
        MiruStreamId streamId,
        List<Integer> expected)
        throws Exception {
        Optional<EWAHCompressedBitmap> unread = miruUnreadTrackingIndex.getUnread(streamId);
        assertFalse(unread.isPresent());

        miruUnreadTrackingIndex.index(streamId, 1);
        miruUnreadTrackingIndex.index(streamId, 3);

        EWAHCompressedBitmap readMask = new EWAHCompressedBitmap();
        readMask.set(1);
        readMask.set(2);
        miruUnreadTrackingIndex.applyRead(streamId, readMask);

        unread = miruUnreadTrackingIndex.getUnread(streamId);
        assertTrue(unread.isPresent());
        assertEquals(unread.get().cardinality(), 1);
        assertTrue(unread.get().get(3));

        EWAHCompressedBitmap unreadMask = new EWAHCompressedBitmap();
        unreadMask.set(1);
        unreadMask.set(3);
        miruUnreadTrackingIndex.applyUnread(streamId, unreadMask);

        unread = miruUnreadTrackingIndex.getUnread(streamId);
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
        EWAHCompressedBitmap unread = miruUnreadTrackingIndex.getUnread(streamId).get();
        assertEquals(unread.cardinality(), 3);

        EWAHCompressedBitmap readMask = new EWAHCompressedBitmap();
        readMask.set(1);
        readMask.set(2);
        readMask.set(5);
        miruUnreadTrackingIndex.applyRead(streamId, readMask);

        unread = miruUnreadTrackingIndex.getUnread(streamId).get();
        assertEquals(unread.cardinality(), 1);
        assertTrue(unread.get(3));
    }

    @DataProvider(name = "miruUnreadTrackingIndexDataProvider")
    public Object[][] miruUnreadTrackingIndexDataProvider() throws Exception {
        return generateUnreadIndexes(new MiruTenantId(new byte[] { 1 }), Collections.<Integer>emptyList(), true);
    }

    @DataProvider(name = "miruUnreadTrackingIndexDataProviderWithoutData")
    public Object[][] miruUnreadTrackingIndexDataProviderWithoutData() throws Exception {
        return generateUnreadIndexes(new MiruTenantId(new byte[] { 1 }), Collections.<Integer>emptyList(), false);
    }

    @DataProvider(name = "miruUnreadTrackingIndexDataProviderWithData")
    public Object[][] miruUnreadTrackingIndexDataProviderWithData() throws Exception {
        return generateUnreadIndexes(new MiruTenantId(new byte[] { 1 }), Arrays.asList(1, 2, 3), true);
    }

    private Object[][] generateUnreadIndexes(MiruTenantId tenantId, List<Integer> data, boolean autoCreate) throws Exception {
        final MiruStreamId streamId = new MiruStreamId(new Id(12_345).toBytes());

        MiruBitmapsEWAH bitmaps = new MiruBitmapsEWAH(4);
        MiruPartitionCoord coord = new MiruPartitionCoord(new MiruTenantId("test".getBytes()), MiruPartitionId.of(0), new MiruHost("localhost", 10000));

        MiruUnreadTrackingIndex<EWAHCompressedBitmap> miruHybridUnreadTrackingIndex = buildHybridContext(4, bitmaps, coord).unreadTrackingIndex;

        if (autoCreate) {
            for (int index : data) {
                miruHybridUnreadTrackingIndex.index(streamId, index);
            }
        }

        MiruUnreadTrackingIndex<EWAHCompressedBitmap> miruOnDiskUnreadTrackingIndex = buildOnDiskContext(4, bitmaps, coord).unreadTrackingIndex;

        if (autoCreate) {
            for (int index : data) {
                miruOnDiskUnreadTrackingIndex.index(streamId, index);
            }
        }

        return new Object[][] {
            { bitmaps, miruHybridUnreadTrackingIndex, streamId, data },
            { bitmaps, miruOnDiskUnreadTrackingIndex, streamId, data }
        };
    }
}
