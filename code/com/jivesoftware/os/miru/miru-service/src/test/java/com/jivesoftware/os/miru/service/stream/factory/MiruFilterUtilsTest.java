package com.jivesoftware.os.miru.service.stream.factory;

import com.google.common.base.Optional;
import com.google.common.collect.Interners;
import com.googlecode.javaewah.EWAHCompressedBitmap;
import com.jivesoftware.os.jive.utils.io.Filer;
import com.jivesoftware.os.jive.utils.io.RandomAccessFiler;
import com.jivesoftware.os.miru.api.activity.schema.DefaultMiruSchemaDefinition;
import com.jivesoftware.os.miru.api.activity.schema.MiruSchema;
import com.jivesoftware.os.miru.api.base.MiruIBA;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.api.base.MiruTermId;
import com.jivesoftware.os.miru.service.bitmap.MiruBitmaps;
import com.jivesoftware.os.miru.service.bitmap.MiruBitmapsEWAH;
import com.jivesoftware.os.miru.service.bitmap.MiruIntIterator;
import com.jivesoftware.os.miru.service.index.BulkExport;
import com.jivesoftware.os.miru.service.index.MiruFilerProvider;
import com.jivesoftware.os.miru.service.index.MiruTimeIndex;
import com.jivesoftware.os.miru.service.index.disk.MiruOnDiskTimeIndex;
import com.jivesoftware.os.miru.service.index.memory.MiruInMemoryTimeIndex;
import com.jivesoftware.os.miru.service.stream.MiruActivityInternExtern;
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
public class MiruFilterUtilsTest {

    @Test(dataProvider = "evenTimeIndexDataProvider")
    public <BM> void testBuildEvenTimeRangeMask(MiruBitmaps<BM> bitmaps, MiruFilterUtils utils, MiruTimeIndex miruTimeIndex) throws Exception {
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

    @Test(dataProvider = "oddTimeIndexDataProvider")
    public <BM> void testBuildOddTimeRangeMask(MiruBitmaps<BM> bitmaps, MiruFilterUtils utils, MiruTimeIndex miruTimeIndex) throws Exception {
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

    @Test(dataProvider = "singleEntryTimeIndexDataProvider")
    public <BM> void testSingleBitTimeRange(MiruBitmaps<BM> bitmaps, MiruFilterUtils utils, MiruTimeIndex miruTimeIndex) {
        BM bitmap = bitmaps.buildTimeRangeMask(miruTimeIndex, 0, Long.MAX_VALUE);

        assertExpectedNumberOfConsecutiveBitsStartingFromN(bitmaps, bitmap, 0, 1);
    }

    private <BM> void assertExpectedNumberOfConsecutiveBitsStartingFromN(MiruBitmaps<BM> bitmaps, BM bitmap, int expectedStartingFrom, int expectedCardinality) {
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

    @DataProvider(name = "evenTimeIndexDataProvider")
    public Object[][] evenTimeIndexDataProvider() throws Exception {

        MiruActivityInternExtern activityInterner = new MiruActivityInternExtern(
                Interners.<MiruIBA>newWeakInterner(), Interners.<MiruTermId>newWeakInterner(),
                Interners.<MiruTenantId>newWeakInterner(), Interners.<String>newWeakInterner());
        MiruFilterUtils miruFilterUtils = new MiruFilterUtils(new MiruBitmapsEWAH(2), activityInterner);

        final int size = (EWAHCompressedBitmap.WORD_IN_BITS * 3) + 1;
        final long[] timestamps = new long[size];
        for (int i = 0; i < size; i++) {
            timestamps[i] = i;
        }

        MiruTenantId tenantId = new MiruTenantId(new byte[]{1});
        MiruInMemoryTimeIndex miruInMemoryTimeIndex = buildInMemoryTimeIndex(tenantId, timestamps);
        MiruOnDiskTimeIndex miruOnDiskTimeIndex = buildOnDiskTimeIndex(tenantId, miruInMemoryTimeIndex);

        return new Object[][] {
            { new MiruBitmapsEWAH(2), miruFilterUtils, miruInMemoryTimeIndex },
            { new MiruBitmapsEWAH(2), miruFilterUtils, miruOnDiskTimeIndex },
        };
    }

    @DataProvider(name = "oddTimeIndexDataProvider")
    public <BM> Object[][] oddTimeIndexDataProvider(MiruBitmaps<BM> bitmaps) throws Exception {
        MiruActivityInternExtern activityInterner = new MiruActivityInternExtern(
                Interners.<MiruIBA>newWeakInterner(), Interners.<MiruTermId>newWeakInterner(),
                Interners.<MiruTenantId>newWeakInterner(), Interners.<String>newWeakInterner());
        MiruFilterUtils miruFilterUtils = new MiruFilterUtils(bitmaps, activityInterner);

        final int size = EWAHCompressedBitmap.WORD_IN_BITS * 3;
        final long[] timestamps = new long[size];
        for (int i = 0; i < size; i++) {
            timestamps[i] = i;
        }

        MiruTenantId tenantId = new MiruTenantId(new byte[]{1});
        MiruInMemoryTimeIndex miruInMemoryTimeIndex = buildInMemoryTimeIndex(tenantId, timestamps);
        MiruOnDiskTimeIndex miruOnDiskTimeIndex = buildOnDiskTimeIndex(tenantId, miruInMemoryTimeIndex);

        return new Object[][] {
            { miruFilterUtils, miruInMemoryTimeIndex },
            { miruFilterUtils, miruOnDiskTimeIndex },
        };
    }

    @DataProvider(name = "singleEntryTimeIndexDataProvider")
    public Object[][] singleEntryTimeIndexDataProvider() throws Exception {
        MiruActivityInternExtern activityInterner = new MiruActivityInternExtern(
                Interners.<MiruIBA>newWeakInterner(), Interners.<MiruTermId>newWeakInterner(),
                Interners.<MiruTenantId>newWeakInterner(), Interners.<String>newWeakInterner());
        MiruFilterUtils miruFilterUtils = new MiruFilterUtils(new MiruBitmapsEWAH(2), activityInterner);

        final long[] timestamps = new long[] { System.currentTimeMillis() };
        MiruTenantId tenantId = new MiruTenantId(new byte[]{1});
        MiruInMemoryTimeIndex miruInMemoryTimeIndex = buildInMemoryTimeIndex(tenantId, timestamps);
        MiruOnDiskTimeIndex miruOnDiskTimeIndex = buildOnDiskTimeIndex(tenantId, miruInMemoryTimeIndex);

        return new Object[][] {
            { new MiruBitmapsEWAH(2), miruFilterUtils, miruInMemoryTimeIndex },
            { new MiruBitmapsEWAH(2), miruFilterUtils, miruOnDiskTimeIndex },
        };
    }

    private MiruInMemoryTimeIndex buildInMemoryTimeIndex(MiruTenantId tenantId, final long[] timestamps) throws Exception {
        MiruInMemoryTimeIndex miruInMemoryTimeIndex = new MiruInMemoryTimeIndex(Optional.<MiruInMemoryTimeIndex.TimeOrderAnomalyStream>absent());
        miruInMemoryTimeIndex.bulkImport(tenantId, new BulkExport<long[]>() {
            @Override
            public long[] bulkExport(MiruTenantId tenantId) throws Exception {
                return timestamps;
            }
        });
        return miruInMemoryTimeIndex;
    }

    private MiruOnDiskTimeIndex buildOnDiskTimeIndex(MiruTenantId tenantId, MiruInMemoryTimeIndex miruInMemoryTimeIndex) throws Exception {
        final File onDisk = Files.createTempFile("onDisk", "timeIndex").toFile();

        MiruOnDiskTimeIndex miruOnDiskTimeIndex = new MiruOnDiskTimeIndex(new MiruFilerProvider() {
            @Override
            public File getBackingFile() {
                return onDisk;
            }

            @Override
            public Filer getFiler(long length) throws IOException {
                return new RandomAccessFiler(onDisk, "rw");
            }
        }, Files.createTempDirectory("timestampToIndex").toFile());
        miruOnDiskTimeIndex.bulkImport(tenantId, miruInMemoryTimeIndex);

        return miruOnDiskTimeIndex;
    }
}
