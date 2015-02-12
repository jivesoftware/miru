package com.jivesoftware.os.miru.service.index;

import com.google.common.collect.Lists;
import com.googlecode.javaewah.EWAHCompressedBitmap;
import com.jivesoftware.os.filer.io.HeapByteBufferFactory;
import com.jivesoftware.os.miru.plugin.bitmap.MiruBitmaps;
import com.jivesoftware.os.miru.plugin.index.MiruFieldIndex;
import com.jivesoftware.os.miru.plugin.index.MiruInvertedIndex;
import com.jivesoftware.os.miru.service.IndexTestUtil;
import com.jivesoftware.os.miru.service.bitmap.AnswerCardinalityLastSetBitmapStorage;
import com.jivesoftware.os.miru.service.bitmap.MiruBitmapsEWAH;
import com.jivesoftware.os.miru.service.index.delta.MiruDeltaInvertedIndex;
import com.jivesoftware.os.miru.service.index.filer.MiruFilerInvertedIndex;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;
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

    @Test(dataProvider = "miruInvertedIndexDataProviderWithData",
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

    @Test(dataProvider = "miruInvertedIndexDataProviderWithData")
    public void testAppend(MiruInvertedIndex<EWAHCompressedBitmap> miruInvertedIndex, List<Integer> ids) throws Exception {
        MiruBitmaps<EWAHCompressedBitmap> bitmaps = new MiruBitmapsEWAH(100);

        int lastId = ids.get(ids.size() - 1);
        miruInvertedIndex.append(lastId + 1);
        miruInvertedIndex.append(lastId + 3);

        for (int id : ids) {
            assertTrue(bitmaps.isSet(miruInvertedIndex.getIndex().get(), id));
        }

        assertTrue(bitmaps.isSet(miruInvertedIndex.getIndex().get(), lastId + 1));
        assertFalse(bitmaps.isSet(miruInvertedIndex.getIndex().get(), lastId + 2));
        assertTrue(bitmaps.isSet(miruInvertedIndex.getIndex().get(), lastId + 3));
        assertFalse(bitmaps.isSet(miruInvertedIndex.getIndex().get(), lastId + 4));
    }

    @Test(dataProvider = "miruInvertedIndexDataProviderWithData")
    public void testRemove(MiruInvertedIndex<EWAHCompressedBitmap> miruInvertedIndex, List<Integer> ids) throws Exception {
        EWAHCompressedBitmap index = miruInvertedIndex.getIndex().get();
        assertEquals(index.cardinality(), ids.size());

        for (int id : ids) {
            assertTrue(index.get(id));
        }

        for (int i = 0; i < ids.size() / 2; i++) {
            miruInvertedIndex.remove(ids.get(i));
        }
        ids = ids.subList(ids.size() / 2, ids.size());

        index = miruInvertedIndex.getIndex().get();
        assertEquals(index.cardinality(), ids.size());
        for (int id : ids) {
            assertTrue(index.get(id));
        }
    }

    @Test(dataProvider = "miruInvertedIndexDataProviderWithData")
    public void testSet(MiruInvertedIndex<EWAHCompressedBitmap> miruInvertedIndex, List<Integer> ids) throws Exception {
        MiruBitmaps<EWAHCompressedBitmap> bitmaps = new MiruBitmapsEWAH(100);

        int lastId = ids.get(ids.size() - 1);

        miruInvertedIndex.append(lastId + 2);
        miruInvertedIndex.set(lastId + 1);

        for (int id : ids) {
            assertTrue(bitmaps.isSet(miruInvertedIndex.getIndex().get(), id));
        }
        assertTrue(bitmaps.isSet(miruInvertedIndex.getIndex().get(), lastId + 1));
        assertTrue(bitmaps.isSet(miruInvertedIndex.getIndex().get(), lastId + 2));
    }

    @Test(dataProvider = "miruInvertedIndexDataProviderWithData")
    public void testSetNonIntermediateBit(MiruInvertedIndex<EWAHCompressedBitmap> miruInvertedIndex, List<Integer> ids) throws Exception {
        MiruBitmaps<EWAHCompressedBitmap> bitmaps = new MiruBitmapsEWAH(100);

        int lastId = ids.get(ids.size() - 1);

        miruInvertedIndex.append(lastId + 1);
        miruInvertedIndex.set(lastId + 2);

        for (int id : ids) {
            assertTrue(bitmaps.isSet(miruInvertedIndex.getIndex().get(), id));
        }
        assertTrue(bitmaps.isSet(miruInvertedIndex.getIndex().get(), lastId + 1));
        assertTrue(bitmaps.isSet(miruInvertedIndex.getIndex().get(), lastId + 2));
    }

    @Test(dataProvider = "miruInvertedIndexDataProviderWithData")
    public void testAndNot(MiruInvertedIndex<EWAHCompressedBitmap> miruInvertedIndex, List<Integer> ids) throws Exception {
        MiruBitmaps<EWAHCompressedBitmap> bitmaps = new MiruBitmapsEWAH(100);

        EWAHCompressedBitmap bitmap = new EWAHCompressedBitmap();
        for (int i = 0; i < ids.size() / 2; i++) {
            bitmap.set(ids.get(i));
        }
        miruInvertedIndex.andNot(bitmap);

        for (int i = 0; i < ids.size() / 2; i++) {
            assertFalse(bitmaps.isSet(miruInvertedIndex.getIndex().get(), ids.get(i)));
        }
        for (int i = ids.size() / 2; i < ids.size(); i++) {
            assertTrue(bitmaps.isSet(miruInvertedIndex.getIndex().get(), ids.get(i)));
        }
    }

    @Test(dataProvider = "miruInvertedIndexDataProviderWithData")
    public void testAndNotToSourceSize(MiruInvertedIndex<EWAHCompressedBitmap> miruInvertedIndex, List<Integer> ids) throws Exception {
        MiruBitmaps<EWAHCompressedBitmap> bitmaps = new MiruBitmapsEWAH(100);

        EWAHCompressedBitmap bitmap = new EWAHCompressedBitmap();
        for (int i = 0; i < ids.size() / 2; i++) {
            bitmap.set(ids.get(i));
        }
        miruInvertedIndex.andNotToSourceSize(Collections.singletonList(bitmap));

        for (int i = 0; i < ids.size() / 2; i++) {
            assertFalse(bitmaps.isSet(miruInvertedIndex.getIndex().get(), ids.get(i)));
        }
        for (int i = ids.size() / 2; i < ids.size(); i++) {
            assertTrue(bitmaps.isSet(miruInvertedIndex.getIndex().get(), ids.get(i)));
        }
    }

    @Test(dataProvider = "miruInvertedIndexDataProviderWithData")
    public void testOr(MiruInvertedIndex<EWAHCompressedBitmap> miruInvertedIndex, List<Integer> ids) throws Exception {
        MiruBitmaps<EWAHCompressedBitmap> bitmaps = new MiruBitmapsEWAH(100);

        int lastId = ids.get(ids.size() - 1);

        miruInvertedIndex.append(lastId + 2);

        EWAHCompressedBitmap bitmap = new EWAHCompressedBitmap();
        bitmap.set(lastId + 1);
        bitmap.set(lastId + 3);
        miruInvertedIndex.or(bitmap);

        for (int id : ids) {
            assertTrue(bitmaps.isSet(miruInvertedIndex.getIndex().get(), id));
        }
        assertTrue(bitmaps.isSet(miruInvertedIndex.getIndex().get(), lastId + 1));
        assertTrue(bitmaps.isSet(miruInvertedIndex.getIndex().get(), lastId + 2));
        assertTrue(bitmaps.isSet(miruInvertedIndex.getIndex().get(), lastId + 3));
    }

    @Test(dataProvider = "miruInvertedIndexDataProviderWithData")
    public void testOrToSourceSize(MiruInvertedIndex<EWAHCompressedBitmap> miruInvertedIndex, List<Integer> ids) throws Exception {
        MiruBitmaps<EWAHCompressedBitmap> bitmaps = new MiruBitmapsEWAH(100);

        int lastId = ids.get(ids.size() - 1);

        miruInvertedIndex.append(lastId + 2);

        EWAHCompressedBitmap bitmap = new EWAHCompressedBitmap();
        bitmap.set(lastId + 1);
        bitmap.set(lastId + 3);
        miruInvertedIndex.orToSourceSize(bitmap);

        for (int id : ids) {
            assertTrue(bitmaps.isSet(miruInvertedIndex.getIndex().get(), id));
        }
        assertTrue(bitmaps.isSet(miruInvertedIndex.getIndex().get(), lastId + 1));
        assertTrue(bitmaps.isSet(miruInvertedIndex.getIndex().get(), lastId + 2));
        assertFalse(bitmaps.isSet(miruInvertedIndex.getIndex().get(), lastId + 3)); // extra bit is ignored
    }

    @DataProvider(name = "miruInvertedIndexDataProviderWithData")
    public Object[][] miruInvertedIndexDataProviderWithData() throws Exception {
        MiruBitmapsEWAH bitmaps = new MiruBitmapsEWAH(100);

        MiruDeltaInvertedIndex<EWAHCompressedBitmap> mergedIndex = buildDeltaInvertedIndex(bitmaps);
        mergedIndex.append(1, 2, 3, 4);
        mergedIndex.merge();

        MiruDeltaInvertedIndex<EWAHCompressedBitmap> unmergedIndex = buildDeltaInvertedIndex(bitmaps);
        unmergedIndex.append(1, 2, 3, 4);

        MiruDeltaInvertedIndex<EWAHCompressedBitmap> partiallyMergedIndex = buildDeltaInvertedIndex(bitmaps);
        partiallyMergedIndex.append(1, 2);
        partiallyMergedIndex.merge();
        partiallyMergedIndex.append(3, 4);

        return new Object[][] {
            { mergedIndex, Arrays.asList(1, 2, 3, 4) },
            { unmergedIndex, Arrays.asList(1, 2, 3, 4) },
            { partiallyMergedIndex, Arrays.asList(1, 2, 3, 4) }
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
