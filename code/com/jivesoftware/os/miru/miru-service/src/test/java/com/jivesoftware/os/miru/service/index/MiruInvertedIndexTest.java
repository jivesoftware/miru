package com.jivesoftware.os.miru.service.index;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.googlecode.javaewah.EWAHCompressedBitmap;
import com.jivesoftware.os.filer.io.FilerIO;
import com.jivesoftware.os.filer.io.HeapByteBufferFactory;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.plugin.bitmap.MiruBitmaps;
import com.jivesoftware.os.miru.plugin.index.MiruFieldIndex;
import com.jivesoftware.os.miru.plugin.index.MiruInvertedIndex;
import com.jivesoftware.os.miru.service.IndexTestUtil;
import com.jivesoftware.os.miru.service.bitmap.AnswerCardinalityLastSetBitmapStorage;
import com.jivesoftware.os.miru.service.bitmap.MiruBitmapsEWAH;
import com.jivesoftware.os.miru.service.index.disk.MiruDeltaInvertedIndex;
import com.jivesoftware.os.miru.service.index.disk.MiruFilerInvertedIndex;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

/**
 *
 */
public class MiruInvertedIndexTest {

    @Test(dataProvider = "miruInvertedIndexDataProvider",
        groups = "slow", enabled = false, description = "Performance test")
    public void testSetId(MiruInvertedIndex<EWAHCompressedBitmap> miruInvertedIndex, int appends, int sets) throws Exception {
        Random r = new Random(1_234);
        int id = 0;
        int[] ids = new int[sets + appends];
        for (int i = 0; i < appends; i++) {
            id += 1 + (r.nextDouble() * 120_000);
            miruInvertedIndex.append(id);
            ids[i] = id;
            if (i % 100_000 == 0) {
                System.out.println("add " + i);
            }
        }

        System.out.println("max id " + id);
        System.out.println("bitmap size " + miruInvertedIndex.getIndex().get().sizeInBytes());
        //Thread.sleep(2000);

        long timestamp = System.currentTimeMillis();
        long subTimestamp = System.currentTimeMillis();
        for (int i = 0; i < sets; i++) {
            miruInvertedIndex.remove(ids[i]);

            id += 1 + (r.nextDouble() * 120);
            miruInvertedIndex.append(id);
            ids[appends + i] = id;

            if (i % 1_000 == 0) {
                //System.out.println("set " + i);
                System.out.println(String.format("set 1000, elapsed = %s, max id = %s, bitmap size = %s",
                    (System.currentTimeMillis() - subTimestamp), id, miruInvertedIndex.getIndex().get().sizeInBytes()));
                subTimestamp = System.currentTimeMillis();
            }
        }
        System.out.println("elapsed: " + (System.currentTimeMillis() - timestamp) + " ms");
    }

    @Test(dataProvider = "miruInvertedIndexDataProvider")
    public void testAppend(MiruInvertedIndex<EWAHCompressedBitmap> miruInvertedIndex) throws Exception {
        MiruBitmaps<EWAHCompressedBitmap> bitmaps = new MiruBitmapsEWAH(100);
        miruInvertedIndex.append(1);
        miruInvertedIndex.append(2);
        miruInvertedIndex.append(3);
        miruInvertedIndex.append(5);
        miruInvertedIndex.append(8);
        assertTrue(bitmaps.isSet(miruInvertedIndex.getIndex().get(), 1));
        assertTrue(bitmaps.isSet(miruInvertedIndex.getIndex().get(), 2));
        assertTrue(bitmaps.isSet(miruInvertedIndex.getIndex().get(), 3));
        assertTrue(bitmaps.isSet(miruInvertedIndex.getIndex().get(), 5));
        assertTrue(bitmaps.isSet(miruInvertedIndex.getIndex().get(), 8));
    }

    @Test(dataProvider = "miruInvertedIndexDataProviderWithData")
    public void testRemove(MiruInvertedIndex<EWAHCompressedBitmap> miruInvertedIndex, Set<Integer> ids) throws Exception {
        EWAHCompressedBitmap index = miruInvertedIndex.getIndex().get();
        assertEquals(index.cardinality(), ids.size());
        for (Integer id : ids) {
            assertTrue(index.get(id));
        }

        miruInvertedIndex.remove(2);
        ids.remove(2);

        index = miruInvertedIndex.getIndex().get();
        assertEquals(index.cardinality(), ids.size());
        for (Integer id : ids) {
            assertTrue(index.get(id));
        }
    }

    @Test(dataProvider = "miruInvertedIndexDataProvider")
    public void testSet(MiruInvertedIndex<EWAHCompressedBitmap> miruInvertedIndex) throws Exception {
        MiruBitmaps<EWAHCompressedBitmap> bitmaps = new MiruBitmapsEWAH(100);
        miruInvertedIndex.append(1);
        miruInvertedIndex.append(5);
        miruInvertedIndex.set(3);
        assertTrue(bitmaps.isSet(miruInvertedIndex.getIndex().get(), 1));
        assertTrue(bitmaps.isSet(miruInvertedIndex.getIndex().get(), 5));
        assertTrue(bitmaps.isSet(miruInvertedIndex.getIndex().get(), 3));
    }

    @Test(dataProvider = "miruInvertedIndexDataProvider")
    public void testSetNonIntermediateBit(MiruInvertedIndex<EWAHCompressedBitmap> miruInvertedIndex) throws Exception {
        MiruBitmaps<EWAHCompressedBitmap> bitmaps = new MiruBitmapsEWAH(100);
        miruInvertedIndex.append(1);
        miruInvertedIndex.set(2);

        assertTrue(bitmaps.isSet(miruInvertedIndex.getIndex().get(), 1));
        assertTrue(bitmaps.isSet(miruInvertedIndex.getIndex().get(), 2));
    }

    @Test(dataProvider = "miruInvertedIndexDataProvider")
    public void testAndNot(MiruInvertedIndex<EWAHCompressedBitmap> miruInvertedIndex) throws Exception {
        MiruBitmaps<EWAHCompressedBitmap> bitmaps = new MiruBitmapsEWAH(100);
        miruInvertedIndex.append(1);
        miruInvertedIndex.append(2);
        miruInvertedIndex.append(3);

        EWAHCompressedBitmap bitmap = new EWAHCompressedBitmap();
        bitmap.set(1);
        bitmap.set(3);
        miruInvertedIndex.andNotToSourceSize(Collections.singletonList(bitmap));

        assertFalse(bitmaps.isSet(miruInvertedIndex.getIndex().get(), 1));
        assertTrue(bitmaps.isSet(miruInvertedIndex.getIndex().get(), 2));
        assertFalse(bitmaps.isSet(miruInvertedIndex.getIndex().get(), 3));
    }

    @Test(dataProvider = "miruInvertedIndexDataProvider")
    public void testOr(MiruInvertedIndex<EWAHCompressedBitmap> miruInvertedIndex) throws Exception {
        MiruBitmaps<EWAHCompressedBitmap> bitmaps = new MiruBitmapsEWAH(100);
        miruInvertedIndex.append(1);
        miruInvertedIndex.append(2);
        miruInvertedIndex.append(3);

        EWAHCompressedBitmap bitmap = new EWAHCompressedBitmap();
        bitmap.set(1);
        bitmap.set(3);
        bitmap.set(5);
        miruInvertedIndex.orToSourceSize(bitmap);

        assertTrue(bitmaps.isSet(miruInvertedIndex.getIndex().get(), 1));
        assertTrue(bitmaps.isSet(miruInvertedIndex.getIndex().get(), 2));
        assertTrue(bitmaps.isSet(miruInvertedIndex.getIndex().get(), 3));
        assertFalse(bitmaps.isSet(miruInvertedIndex.getIndex().get(), 4));
        assertFalse(bitmaps.isSet(miruInvertedIndex.getIndex().get(), 5)); // extra bit is ignored
    }

    @DataProvider(name = "miruInvertedIndexDataProviderWithScale")
    public Object[][] miruInvertedIndexDataProviderWithScale() throws Exception {
        MiruBitmapsEWAH bitmaps = new MiruBitmapsEWAH(100);
        int memoryAppends = 1_000;
        int memorySets = 100_000;
        int diskAppends = 100;
        int diskSets = 1_000;

        return new Object[][] {
            { buildFilerInvertedIndex(bitmaps), diskAppends, diskSets },
            { buildDeltaInvertedIndex(bitmaps), diskAppends, diskSets }
        };
    }

    @DataProvider(name = "miruInvertedIndexDataProvider")
    public Object[][] miruInvertedIndexDataProvider() throws Exception {
        MiruBitmapsEWAH bitmaps = new MiruBitmapsEWAH(100);
        return new Object[][] {
            { buildFilerInvertedIndex(bitmaps) },
            { buildDeltaInvertedIndex(bitmaps) }
        };
    }

    @DataProvider(name = "miruInvertedIndexDataProviderWithData")
    public Object[][] miruInvertedIndexDataProviderWithData() throws Exception {
        MiruTenantId tenantId = new MiruTenantId(FilerIO.intBytes(1));
        MiruBitmapsEWAH bitmaps = new MiruBitmapsEWAH(100);

        MiruFilerInvertedIndex<EWAHCompressedBitmap> miruFilerInvertedIndex = buildFilerInvertedIndex(bitmaps);
        miruFilerInvertedIndex.append(1, 2, 3);

        MiruDeltaInvertedIndex<EWAHCompressedBitmap> miruDeltaInvertedIndex = buildDeltaInvertedIndex(bitmaps);
        miruDeltaInvertedIndex.append(1, 2, 3);

        return new Object[][] {
            { miruFilerInvertedIndex, Sets.newHashSet(1, 2, 3) },
            { miruDeltaInvertedIndex, Sets.newHashSet(1, 2, 3) }
        };
    }

    private <BM> MiruFilerInvertedIndex<BM> buildFilerInvertedIndex(MiruBitmaps<BM> bitmaps) throws Exception {
        return new MiruFilerInvertedIndex<>(bitmaps,
            null,
            new MiruFieldIndex.IndexKey(0, new byte[] { 0 }),
            IndexTestUtil.buildKeyedFilerStore("index",
                IndexTestUtil.buildByteBufferBackedChunkStores(4, new HeapByteBufferFactory(), 4_096)),
            -1,
            new Object());
    }

    private <BM> MiruDeltaInvertedIndex<BM> buildDeltaInvertedIndex(MiruBitmaps<BM> bitmaps) throws Exception {
        return new MiruDeltaInvertedIndex<>(bitmaps,
            buildFilerInvertedIndex(bitmaps),
            new MiruDeltaInvertedIndex.Delta<BM>());
    }

    @Test(groups = "slow", enabled = false, description = "Concurrency test")
    public void testConcurrency() throws Exception {
        MiruBitmapsEWAH bitmaps = new MiruBitmapsEWAH(100);
        ExecutorService executorService = Executors.newFixedThreadPool(8);

        final MiruFilerInvertedIndex<EWAHCompressedBitmap> invertedIndex = buildFilerInvertedIndex(bitmaps);
        final AtomicInteger idProvider = new AtomicInteger();
        final AtomicInteger done = new AtomicInteger();
        final int runs = 10_000;
        final Random random = new Random();

        List<Future<?>> futures = Lists.newArrayListWithCapacity(8);

        futures.add(executorService.submit(new Runnable() {
            @Override
            public void run() {
                while (done.get() < 7) {
                    int id = idProvider.incrementAndGet();
                    if (id == Integer.MAX_VALUE) {
                        System.out.println("appender hit max value");
                        break;
                    }
                    if (random.nextBoolean()) {
                        try {
                            invertedIndex.append(id);
                        } catch (Exception e) {
                            throw new RuntimeException(e);
                        }
                    }
                }
                try {
                    EWAHCompressedBitmap index = invertedIndex.getIndex().get();
                    System.out.println("appender is done, final cardinality=" + index.cardinality() + " bits=" + index.sizeInBits());
                } catch (Exception e) {
                    e.printStackTrace();
                    throw new RuntimeException(e);
                }
            }
        }));
        System.out.println("started appender");

        for (int i = 0; i < 7; i++) {
            futures.add(executorService.submit(new Runnable() {
                @Override
                public void run() {
                    EWAHCompressedBitmap other = new EWAHCompressedBitmap();
                    int r = 0;
                    for (int i = 0; i < runs; i++) {
                        int size = idProvider.get();
                        while (r < size) {
                            if (random.nextBoolean()) {
                                other.set(r);
                            }
                            r++;
                        }

                        try {
                            EWAHCompressedBitmap index = invertedIndex.getIndex().get();
                            EWAHCompressedBitmap container = new EWAHCompressedBitmap();
                            AnswerCardinalityLastSetBitmapStorage answer = new AnswerCardinalityLastSetBitmapStorage(container);
                            index.andToContainer(other, answer);
                        } catch (Exception e) {
                            done.incrementAndGet();
                            e.printStackTrace();
                            throw new RuntimeException(e);
                        }
                    }

                    System.out.println("aggregator is done, final cardinality=" + other.cardinality() + " bits=" + other.sizeInBits());
                    done.incrementAndGet();
                }
            }));
            System.out.println("started aggregators");
        }

        for (Future<?> future : futures) {
            future.get();
            System.out.println("got a future");
        }

        System.out.println("all done");
    }

    @Test(groups = "slow", enabled = false, description = "Performance test")
    public void testInMemoryAppenderSpeed() throws Exception {
        MiruFilerInvertedIndex<EWAHCompressedBitmap> invertedIndex = buildFilerInvertedIndex(new MiruBitmapsEWAH(100));

        Random r = new Random(1_249_871_239_817_231_827l);
        long t = System.currentTimeMillis();
        for (int i = 0; i < 1_000_000; i++) {
            if (r.nextBoolean()) {
                invertedIndex.append(i);
            }
        }

        long elapsed = System.currentTimeMillis() - t;
        EWAHCompressedBitmap index = invertedIndex.getIndex().get();
        System.out.println("cardinality=" + index.cardinality() + " bits=" + index.sizeInBits() + " bytes=" + index.sizeInBytes() + " elapsed=" + elapsed);
    }
}
