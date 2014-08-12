package com.jivesoftware.os.miru.service.index;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Interners;
import com.google.common.collect.Maps;
import com.googlecode.javaewah.EWAHCompressedBitmap;
import com.jivesoftware.os.miru.api.query.filter.MiruAuthzExpression;
import com.jivesoftware.os.miru.service.index.disk.MiruOnDiskAuthzIndex;
import com.jivesoftware.os.miru.service.index.memory.MiruInMemoryAuthzIndex;
import com.jivesoftware.os.miru.service.index.memory.MiruInMemoryInvertedIndex;
import com.jivesoftware.os.jive.utils.chunk.store.ChunkStore;
import com.jivesoftware.os.jive.utils.chunk.store.ChunkStoreInitializer;
import com.jivesoftware.os.jive.utils.io.FilerIO;
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
    public void storeAndGetAuthz(MiruAuthzIndex miruAuthzIndex, MiruAuthzUtils miruAuthzUtils, Map<Integer, EWAHCompressedBitmap> bitsIn) throws Exception {
        for (Map.Entry<Integer, EWAHCompressedBitmap> entry : bitsIn.entrySet()) {
            String authz = miruAuthzUtils.encode(FilerIO.longBytes((long) entry.getKey()));
            MiruAuthzExpression miruAuthzExpression = new MiruAuthzExpression(ImmutableList.of(authz));

            EWAHCompressedBitmap bitsOut = miruAuthzIndex.getCompositeAuthz(miruAuthzExpression);

            assertEquals(bitsOut, entry.getValue());
        }
    }

    @DataProvider(name = "miruAuthzIndexDataProviderWithData")
    public Object[][] miruAuthzIndexDataProvider() throws Exception {
        MiruAuthzUtils miruAuthzUtils = new MiruAuthzUtils(8192);

        final Map<Integer, EWAHCompressedBitmap> smallBitsIn = Maps.newHashMap();
        final Map<Integer, EWAHCompressedBitmap> largeBitsIn = Maps.newHashMap();
        final InvertedIndexData smallInvertedIndexData = buildInMemoryInvertedIndexes(smallBitsIn, miruAuthzUtils, 10);
        final InvertedIndexData largeInvertedIndexData = buildInMemoryInvertedIndexes(largeBitsIn, miruAuthzUtils, 100);

        // Create in-memory authz index
        MiruInMemoryAuthzIndex smallMiruInMemoryAuthzIndex = new MiruInMemoryAuthzIndex(cache(miruAuthzUtils, 10));
        MiruInMemoryAuthzIndex largeMiruInMemoryAuthzIndex = new MiruInMemoryAuthzIndex(cache(miruAuthzUtils, 10));

        // Import items for test
        smallMiruInMemoryAuthzIndex.bulkImport(new BulkExport<Map<String, MiruInvertedIndex>>() {
            @Override
            public Map<String, MiruInvertedIndex> bulkExport() throws Exception {
                return smallInvertedIndexData.getImportItems();
            }
        });
        largeMiruInMemoryAuthzIndex.bulkImport(new BulkExport<Map<String, MiruInvertedIndex>>() {
            @Override
            public Map<String, MiruInvertedIndex> bulkExport() throws Exception {
                return largeInvertedIndexData.getImportItems();
            }
        });

        File mapDir = Files.createTempDirectory("map").toFile();
        File swapDir = Files.createTempDirectory("swap").toFile();
        Path chunksDir = Files.createTempDirectory("chunks");
        File chunks = new File(chunksDir.toFile(), "chunks.data");
        ChunkStore chunkStore = new ChunkStoreInitializer().initialize(chunks.getAbsolutePath(), 16384, false);
        MiruOnDiskAuthzIndex smallMiruOnDiskAuthzIndex = new MiruOnDiskAuthzIndex(mapDir, swapDir, chunkStore, cache(miruAuthzUtils, 10));
        smallMiruOnDiskAuthzIndex.bulkImport(smallMiruInMemoryAuthzIndex);

        return new Object[][] {
            { smallMiruInMemoryAuthzIndex, miruAuthzUtils, smallBitsIn },
            { largeMiruInMemoryAuthzIndex, miruAuthzUtils, largeBitsIn },
            { smallMiruOnDiskAuthzIndex, miruAuthzUtils, smallBitsIn }
        };
    }

    private MiruAuthzCache cache(MiruAuthzUtils miruAuthzUtils, int maximumSize) {
        Cache<VersionedAuthzExpression, EWAHCompressedBitmap> cache = CacheBuilder.newBuilder()
            .maximumSize(maximumSize)
            .expireAfterAccess(1, TimeUnit.MINUTES)
            .build();
        return new MiruAuthzCache(cache, Interners.<String>newWeakInterner(), miruAuthzUtils);
    }

    private InvertedIndexData buildInMemoryInvertedIndexes(Map<Integer, EWAHCompressedBitmap> bitsIn, MiruAuthzUtils miruAuthzUtils, int size) {
        Map<String, MiruInvertedIndex> importItems = Maps.newHashMap();

        for (int i = 1; i <= size; i++) {
            String authz = miruAuthzUtils.encode(FilerIO.longBytes((long) i));
            EWAHCompressedBitmap bits = new EWAHCompressedBitmap();
            bits.set(i);
            bits.set(10 * i);
            bits.set(100 * i);
            bits.set(1_000 * i);
            bits.set(10_000 * i);
            bits.set(100_000 * i);
            bits.set(1_000_000 * i);

            bitsIn.put(i, bits);
            importItems.put(authz, new MiruInMemoryInvertedIndex(bits));
        }

        return new InvertedIndexData(bitsIn, importItems);
    }

    private class InvertedIndexData {
        private final Map<Integer, EWAHCompressedBitmap> bitsIn;
        private final Map<String, MiruInvertedIndex> importItems;

        private InvertedIndexData(Map<Integer, EWAHCompressedBitmap> bitsIn, Map<String, MiruInvertedIndex> importItems) {
            this.bitsIn = bitsIn;
            this.importItems = importItems;
        }

        public Map<Integer, EWAHCompressedBitmap> getBitsIn() {
            return bitsIn;
        }

        public Map<String, MiruInvertedIndex> getImportItems() {
            return importItems;
        }
    }
}
