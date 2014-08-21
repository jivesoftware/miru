package com.jivesoftware.os.miru.service.index;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Interners;
import com.google.common.collect.Maps;
import com.googlecode.javaewah.EWAHCompressedBitmap;
import com.jivesoftware.os.jive.utils.chunk.store.ChunkStore;
import com.jivesoftware.os.jive.utils.chunk.store.ChunkStoreInitializer;
import com.jivesoftware.os.jive.utils.io.FilerIO;
import com.jivesoftware.os.miru.api.query.filter.MiruAuthzExpression;
import com.jivesoftware.os.miru.service.bitmap.MiruBitmaps;
import com.jivesoftware.os.miru.service.bitmap.MiruBitmapsEWAH;
import com.jivesoftware.os.miru.service.index.auth.MiruAuthzCache;
import com.jivesoftware.os.miru.service.index.auth.MiruAuthzIndex;
import com.jivesoftware.os.miru.service.index.auth.MiruAuthzUtils;
import com.jivesoftware.os.miru.service.index.auth.VersionedAuthzExpression;
import com.jivesoftware.os.miru.service.index.disk.MiruOnDiskAuthzIndex;
import com.jivesoftware.os.miru.service.index.memory.MiruInMemoryAuthzIndex;
import com.jivesoftware.os.miru.service.index.memory.MiruInMemoryInvertedIndex;
import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;

public class MiruAuthzIndexTest {

    @Test(dataProvider = "miruAuthzIndexDataProviderWithData")
    public void storeAndGetAuthz(MiruAuthzIndex<EWAHCompressedBitmap> miruAuthzIndex, MiruAuthzUtils miruAuthzUtils, Map<Integer, EWAHCompressedBitmap> bitsIn) throws Exception {
        for (Map.Entry<Integer, EWAHCompressedBitmap> entry : bitsIn.entrySet()) {
            String authz = miruAuthzUtils.encode(FilerIO.longBytes((long) entry.getKey()));
            MiruAuthzExpression miruAuthzExpression = new MiruAuthzExpression(ImmutableList.of(authz));

            EWAHCompressedBitmap bitsOut = miruAuthzIndex.getCompositeAuthz(miruAuthzExpression);

            assertEquals(bitsOut, entry.getValue());
        }
    }

    @DataProvider(name = "miruAuthzIndexDataProviderWithData")
    public Object[][] miruAuthzIndexDataProvider() throws Exception {
        MiruBitmapsEWAH bitmaps = new MiruBitmapsEWAH(8192);
        MiruAuthzUtils<EWAHCompressedBitmap> miruAuthzUtils = new MiruAuthzUtils<>(bitmaps);

        final Map<Integer, EWAHCompressedBitmap> smallBitsIn = Maps.newHashMap();
        final Map<Integer, EWAHCompressedBitmap> largeBitsIn = Maps.newHashMap();
        final InvertedIndexData<EWAHCompressedBitmap> smallInvertedIndexData = buildInMemoryInvertedIndexes(bitmaps, smallBitsIn, miruAuthzUtils, 10);
        final InvertedIndexData<EWAHCompressedBitmap> largeInvertedIndexData = buildInMemoryInvertedIndexes(bitmaps, largeBitsIn, miruAuthzUtils, 100);

        // Create in-memory authz index
        MiruInMemoryAuthzIndex<EWAHCompressedBitmap> smallMiruInMemoryAuthzIndex = new MiruInMemoryAuthzIndex<>(bitmaps, cache(bitmaps, miruAuthzUtils, 10));
        MiruInMemoryAuthzIndex<EWAHCompressedBitmap> largeMiruInMemoryAuthzIndex = new MiruInMemoryAuthzIndex<>(bitmaps, cache(bitmaps, miruAuthzUtils, 10));

        // Import items for test
        smallMiruInMemoryAuthzIndex.bulkImport(new BulkExport<Map<String, MiruInvertedIndex<EWAHCompressedBitmap>>>() {
            @Override
            public Map<String, MiruInvertedIndex<EWAHCompressedBitmap>> bulkExport() throws Exception {
                return smallInvertedIndexData.getImportItems();
            }
        });
        largeMiruInMemoryAuthzIndex.bulkImport(new BulkExport<Map<String, MiruInvertedIndex<EWAHCompressedBitmap>>>() {
            @Override
            public Map<String, MiruInvertedIndex<EWAHCompressedBitmap>> bulkExport() throws Exception {
                return largeInvertedIndexData.getImportItems();
            }
        });

        File mapDir = Files.createTempDirectory("map").toFile();
        File swapDir = Files.createTempDirectory("swap").toFile();
        Path chunksDir = Files.createTempDirectory("chunks");
        File chunks = new File(chunksDir.toFile(), "chunks.data");
        ChunkStore chunkStore = new ChunkStoreInitializer().initialize(chunks.getAbsolutePath(), 16384, false);
        MiruOnDiskAuthzIndex<EWAHCompressedBitmap> smallMiruOnDiskAuthzIndex = new MiruOnDiskAuthzIndex<>(bitmaps, mapDir, swapDir, chunkStore, cache(bitmaps, miruAuthzUtils, 10));
        smallMiruOnDiskAuthzIndex.bulkImport(smallMiruInMemoryAuthzIndex);

        return new Object[][] {
            { smallMiruInMemoryAuthzIndex, miruAuthzUtils, smallBitsIn },
            { largeMiruInMemoryAuthzIndex, miruAuthzUtils, largeBitsIn },
            { smallMiruOnDiskAuthzIndex, miruAuthzUtils, smallBitsIn }
        };
    }

    private <BM> MiruAuthzCache<BM> cache(MiruBitmaps<BM> bitmaps, MiruAuthzUtils<BM> miruAuthzUtils, int maximumSize) {
        Cache<VersionedAuthzExpression, BM> cache = CacheBuilder.newBuilder()
            .maximumSize(maximumSize)
            .expireAfterAccess(1, TimeUnit.MINUTES)
            .build();
        return new MiruAuthzCache<BM>(bitmaps, cache, Interners.<String>newWeakInterner(), miruAuthzUtils);
    }

    private <BM> InvertedIndexData<BM> buildInMemoryInvertedIndexes(MiruBitmaps<BM> bitmaps, Map<Integer, BM> bitsIn, MiruAuthzUtils<BM> miruAuthzUtils, int size) throws Exception {
        Map<String, MiruInvertedIndex> importItems = Maps.newHashMap();

        for (int i = 1; i <= size; i++) {
            String authz = miruAuthzUtils.encode(FilerIO.longBytes((long) i));
            BM bits = bitmaps.create();
            bitmaps.set(bits, i);
            bitmaps.set(bits, 10 * i);
            bitmaps.set(bits, 100 * i);
            bitmaps.set(bits, 1_000 * i);
            bitmaps.set(bits, 10_000 * i);
            bitmaps.set(bits, 100_000 * i);
            bitmaps.set(bits, 1_000_000 * i);
            bitsIn.put(i, bits);

            MiruInvertedIndex<BM> index = new MiruInMemoryInvertedIndex(bitmaps);
            index.or(bits);
            importItems.put(authz, index);
        }

        return new InvertedIndexData(bitsIn, importItems);
    }

    private static class InvertedIndexData<BM2> {
        private final Map<Integer, BM2> bitsIn;
        private final Map<String, MiruInvertedIndex<BM2>> importItems;

        private InvertedIndexData(Map<Integer, BM2> bitsIn, Map<String, MiruInvertedIndex<BM2>> importItems) {
            this.bitsIn = bitsIn;
            this.importItems = importItems;
        }

        public Map<Integer, BM2> getBitsIn() {
            return bitsIn;
        }

        public Map<String, MiruInvertedIndex<BM2>> getImportItems() {
            return importItems;
        }
    }
}
