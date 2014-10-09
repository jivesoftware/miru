package com.jivesoftware.os.miru.service.bitmap;

import com.google.common.base.Optional;
import com.googlecode.javaewah.EWAHCompressedBitmap;
import com.jivesoftware.os.filer.io.Filer;
import com.jivesoftware.os.filer.io.RandomAccessFiler;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.plugin.bitmap.MiruBitmaps;
import com.jivesoftware.os.miru.plugin.bitmap.MiruIntIterator;
import com.jivesoftware.os.miru.plugin.index.MiruTimeIndex;
import com.jivesoftware.os.miru.service.index.MiruFilerProvider;
import com.jivesoftware.os.miru.service.index.disk.MiruOnDiskTimeIndex;
import com.jivesoftware.os.miru.service.index.memory.MiruInMemoryTimeIndex;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.fail;

/**
 *
 */
public class MiruBitmapsTimeRangeTest {

    @Test (dataProvider = "evenTimeIndexDataProvider")
    public <BM> void testBuildEvenTimeRangeMask(MiruBitmaps<BM> bitmaps, MiruTimeIndex miruTimeIndex) throws Exception {
        final int size = (EWAHCompressedBitmap.WORD_IN_BITS * 3) + 1;
        for (int lower = 0; lower <= size / 2; lower++) {
            int upper = size - 1 - lower;

            BM bitmap = bitmaps.buildTimeRangeMask(miruTimeIndex, lower, upper);
            if (lower == 0) {
                // 0 case is inclusive due to ambiguity
                assertExpectedNumberOfConsecutiveBitsStartingFromN(bitmaps, bitmap, lower, size - 2 * lower);
            } else if (lower == upper) {
                // the lower and upper are the same so there should be nothing left
                assertExpectedNumberOfConsecutiveBitsStartingFromN(bitmaps, bitmap, -1, 0);
            } else {
                assertExpectedNumberOfConsecutiveBitsStartingFromN(bitmaps, bitmap, lower + 1, size - 1 - 2 * lower);
            }
        }
    }

    @Test (dataProvider = "oddTimeIndexDataProvider")
    public <BM> void testBuildOddTimeRangeMask(MiruBitmaps<BM> bitmaps, MiruTimeIndex miruTimeIndex) throws Exception {
        final int size = EWAHCompressedBitmap.WORD_IN_BITS * 3;
        for (int lower = 0; lower < size / 2; lower++) {
            int upper = size - 1 - lower;

            BM bitmap = bitmaps.buildTimeRangeMask(miruTimeIndex, lower, upper);
            if (lower == 0) {
                // 0 case is inclusive due to ambiguity
                assertExpectedNumberOfConsecutiveBitsStartingFromN(bitmaps, bitmap, lower, size - 2 * lower);
            } else if (lower == upper) {
                fail();
            } else {
                assertExpectedNumberOfConsecutiveBitsStartingFromN(bitmaps, bitmap, lower + 1, size - 1 - 2 * lower);
            }
        }
    }

    @Test (dataProvider = "singleEntryTimeIndexDataProvider")
    public <BM> void testSingleBitTimeRange(MiruBitmaps<BM> bitmaps, MiruTimeIndex miruTimeIndex) {
        BM bitmap = bitmaps.buildTimeRangeMask(miruTimeIndex, 0, Long.MAX_VALUE);

        assertExpectedNumberOfConsecutiveBitsStartingFromN(bitmaps, bitmap, 0, 1);
    }

    private <BM> void assertExpectedNumberOfConsecutiveBitsStartingFromN(MiruBitmaps<BM> bitmaps, BM bitmap, int expectedStartingFrom,
        int expectedCardinality) {
        int last = -1;
        int cardinality = 0;
        int startingFrom = -1;
        MiruIntIterator iter = bitmaps.intIterator(bitmap);
        while (iter.hasNext()) {
            int current = iter.next();
            if (last < 0) {
                startingFrom = current;
            } else if (last >= 0) {
                assertEquals(current - last, 1);
            }
            last = current;
            cardinality++;
        }
        assertEquals(startingFrom, expectedStartingFrom);
        assertEquals(cardinality, expectedCardinality);
    }

    @DataProvider (name = "evenTimeIndexDataProvider")
    public Object[][] evenTimeIndexDataProvider() throws Exception {

        final int size = (EWAHCompressedBitmap.WORD_IN_BITS * 3) + 1;
        final long[] timestamps = new long[size];
        for (int i = 0; i < size; i++) {
            timestamps[i] = i;
        }

        MiruTenantId tenantId = new MiruTenantId(new byte[]{ 1 });
        MiruInMemoryTimeIndex miruInMemoryTimeIndex = buildInMemoryTimeIndex(tenantId, timestamps);
        MiruOnDiskTimeIndex miruOnDiskTimeIndex = buildOnDiskTimeIndex(tenantId, miruInMemoryTimeIndex);

        return new Object[][]{
            { new MiruBitmapsEWAH(2), miruInMemoryTimeIndex },
            { new MiruBitmapsEWAH(2), miruOnDiskTimeIndex }, };
    }

    @DataProvider (name = "oddTimeIndexDataProvider")
    public <BM> Object[][] oddTimeIndexDataProvider(MiruBitmaps<BM> bitmaps) throws Exception {

        final int size = EWAHCompressedBitmap.WORD_IN_BITS * 3;
        final long[] timestamps = new long[size];
        for (int i = 0; i < size; i++) {
            timestamps[i] = i;
        }

        MiruTenantId tenantId = new MiruTenantId(new byte[]{ 1 });
        MiruInMemoryTimeIndex miruInMemoryTimeIndex = buildInMemoryTimeIndex(tenantId, timestamps);
        MiruOnDiskTimeIndex miruOnDiskTimeIndex = buildOnDiskTimeIndex(tenantId, miruInMemoryTimeIndex);

        return new Object[][]{
            { new MiruBitmapsEWAH(2), miruInMemoryTimeIndex },
            { new MiruBitmapsEWAH(2), miruOnDiskTimeIndex }, };
    }

    @DataProvider (name = "singleEntryTimeIndexDataProvider")
    public Object[][] singleEntryTimeIndexDataProvider() throws Exception {

        final long[] timestamps = new long[]{ System.currentTimeMillis() };
        MiruTenantId tenantId = new MiruTenantId(new byte[]{ 1 });
        MiruInMemoryTimeIndex miruInMemoryTimeIndex = buildInMemoryTimeIndex(tenantId, timestamps);
        MiruOnDiskTimeIndex miruOnDiskTimeIndex = buildOnDiskTimeIndex(tenantId, miruInMemoryTimeIndex);

        return new Object[][]{
            { new MiruBitmapsEWAH(2), miruInMemoryTimeIndex },
            { new MiruBitmapsEWAH(2), miruOnDiskTimeIndex }, };
    }

    private MiruInMemoryTimeIndex buildInMemoryTimeIndex(MiruTenantId tenantId, final long[] timestamps) throws Exception {
        MiruInMemoryTimeIndex miruInMemoryTimeIndex = new MiruInMemoryTimeIndex(Optional.<MiruInMemoryTimeIndex.TimeOrderAnomalyStream>absent());
        for (long timestamp : timestamps) {
            miruInMemoryTimeIndex.nextId(timestamp);
        }
        return miruInMemoryTimeIndex;
    }

    private MiruOnDiskTimeIndex buildOnDiskTimeIndex(MiruTenantId tenantId, MiruInMemoryTimeIndex miruInMemoryTimeIndex) throws Exception {
        final File onDisk = Files.createTempFile("onDisk", "timeIndex").toFile();

        MiruFilerProvider filerProvider = new MiruFilerProvider() {
            @Override
            public File getBackingFile() {
                return onDisk;
            }

            @Override
            public Filer getFiler(long length) throws IOException {
                return new RandomAccessFiler(onDisk, "rw");
            }
        };
        String[] mapDirectories = {
            Files.createTempDirectory("timestampToIndex").toFile().getAbsolutePath(),
            Files.createTempDirectory("timestampToIndex").toFile().getAbsolutePath()
        };
        MiruOnDiskTimeIndex miruOnDiskTimeIndex = new MiruOnDiskTimeIndex(filerProvider, mapDirectories);
        miruOnDiskTimeIndex.bulkImport(tenantId, miruInMemoryTimeIndex);

        return miruOnDiskTimeIndex;
    }
}
