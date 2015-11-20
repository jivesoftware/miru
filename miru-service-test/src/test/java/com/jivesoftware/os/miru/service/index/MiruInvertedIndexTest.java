package com.jivesoftware.os.miru.service.index;

import com.google.common.collect.Lists;
import com.jivesoftware.os.filer.io.HeapByteBufferFactory;
import com.jivesoftware.os.miru.bitmaps.ewah.AnswerCardinalityLastSetBitmapStorage;
import com.jivesoftware.os.miru.bitmaps.roaring5.buffer.MiruBitmapsRoaringBuffer;
import com.jivesoftware.os.miru.plugin.bitmap.MiruBitmaps;
import com.jivesoftware.os.miru.plugin.index.MiruFieldIndex;
import com.jivesoftware.os.miru.plugin.index.MiruInvertedIndex;
import com.jivesoftware.os.miru.service.IndexTestUtil;
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
import org.roaringbitmap.RoaringAggregation;
import org.roaringbitmap.buffer.MutableRoaringBitmap;
import org.roaringbitmap.buffer.RoaringBufferAggregation;
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
    public void testSetId(MiruInvertedIndex<MutableRoaringBitmap> miruInvertedIndex, int appends, int sets) throws Exception {
        byte[] primitiveBuffer = new byte[8];
        Random r = new Random(1_234);
        int id = 0;
        int[] ids = new int[sets + appends];
        for (int i = 0; i < appends; i++) {
            id += 1 + (r.nextDouble() * 120_000);
            miruInvertedIndex.append(primitiveBuffer, id);
            ids[i] = id;
            if (i % 100_000 == 0) {
                System.out.println("add " + i);
            }
        }

        System.out.println("max id " + id);
        System.out.println("bitmap size " + miruInvertedIndex.getIndex(primitiveBuffer).get().getSizeInBytes());
        //Thread.sleep(2000);

        long timestamp = System.currentTimeMillis();
        long subTimestamp = System.currentTimeMillis();
        for (int i = 0; i < sets; i++) {
            miruInvertedIndex.remove(ids[i], primitiveBuffer);

            id += 1 + (r.nextDouble() * 120);
            miruInvertedIndex.append(primitiveBuffer, id);
            ids[appends + i] = id;

            if (i % 1_000 == 0) {
                //System.out.println("set " + i);
                System.out.println(String.format("set 1000, elapsed = %s, max id = %s, bitmap size = %s",
                    (System.currentTimeMillis() - subTimestamp), id, miruInvertedIndex.getIndex(primitiveBuffer).get().getSizeInBytes()));
                subTimestamp = System.currentTimeMillis();
            }
        }
        System.out.println("elapsed: " + (System.currentTimeMillis() - timestamp) + " ms");
    }

    @Test(dataProvider = "miruInvertedIndexDataProviderWithData")
    public void testAppend(MiruInvertedIndex<MutableRoaringBitmap> miruInvertedIndex, List<Integer> ids) throws Exception {
        byte[] primitiveBuffer = new byte[8];
        MiruBitmaps<MutableRoaringBitmap> bitmaps = new MiruBitmapsRoaringBuffer();

        int lastId = ids.get(ids.size() - 1);
        miruInvertedIndex.append(primitiveBuffer, lastId + 1);
        miruInvertedIndex.append(primitiveBuffer, lastId + 3);

        for (int id : ids) {
            assertTrue(bitmaps.isSet(miruInvertedIndex.getIndex(primitiveBuffer).get(), id));
        }

        assertTrue(bitmaps.isSet(miruInvertedIndex.getIndex(primitiveBuffer).get(), lastId + 1));
        assertFalse(bitmaps.isSet(miruInvertedIndex.getIndex(primitiveBuffer).get(), lastId + 2));
        assertTrue(bitmaps.isSet(miruInvertedIndex.getIndex(primitiveBuffer).get(), lastId + 3));
        assertFalse(bitmaps.isSet(miruInvertedIndex.getIndex(primitiveBuffer).get(), lastId + 4));
    }

    @Test(dataProvider = "miruInvertedIndexDataProviderWithData")
    public void testRemove(MiruInvertedIndex<MutableRoaringBitmap> miruInvertedIndex, List<Integer> ids) throws Exception {
        byte[] primitiveBuffer = new byte[8];
        MutableRoaringBitmap index = miruInvertedIndex.getIndex(primitiveBuffer).get();
        assertEquals(index.getCardinality(), ids.size());

        for (int id : ids) {
            assertTrue(index.contains(id));
        }

        for (int i = 0; i < ids.size() / 2; i++) {
            miruInvertedIndex.remove(ids.get(i), primitiveBuffer);
        }
        ids = ids.subList(ids.size() / 2, ids.size());

        index = miruInvertedIndex.getIndex(primitiveBuffer).get();
        assertEquals(index.getCardinality(), ids.size());
        for (int id : ids) {
            assertTrue(index.contains(id));
        }
    }

    @Test(dataProvider = "miruInvertedIndexDataProviderWithData")
    public void testSet(MiruInvertedIndex<MutableRoaringBitmap> miruInvertedIndex, List<Integer> ids) throws Exception {
        byte[] primitiveBuffer = new byte[8];
        MiruBitmaps<MutableRoaringBitmap> bitmaps = new MiruBitmapsRoaringBuffer();

        int lastId = ids.get(ids.size() - 1);

        miruInvertedIndex.append(primitiveBuffer, lastId + 2);
        miruInvertedIndex.set(primitiveBuffer, lastId + 1);

        for (int id : ids) {
            assertTrue(bitmaps.isSet(miruInvertedIndex.getIndex(primitiveBuffer).get(), id));
        }
        assertTrue(bitmaps.isSet(miruInvertedIndex.getIndex(primitiveBuffer).get(), lastId + 1));
        assertTrue(bitmaps.isSet(miruInvertedIndex.getIndex(primitiveBuffer).get(), lastId + 2));
    }

    @Test(dataProvider = "miruInvertedIndexDataProviderWithData")
    public void testSetNonIntermediateBit(MiruInvertedIndex<MutableRoaringBitmap> miruInvertedIndex, List<Integer> ids) throws Exception {
        byte[] primitiveBuffer = new byte[8];
        MiruBitmaps<MutableRoaringBitmap> bitmaps = new MiruBitmapsRoaringBuffer();

        int lastId = ids.get(ids.size() - 1);

        miruInvertedIndex.append(primitiveBuffer, lastId + 1);
        miruInvertedIndex.set(primitiveBuffer, lastId + 2);

        for (int id : ids) {
            assertTrue(bitmaps.isSet(miruInvertedIndex.getIndex(primitiveBuffer).get(), id));
        }
        assertTrue(bitmaps.isSet(miruInvertedIndex.getIndex(primitiveBuffer).get(), lastId + 1));
        assertTrue(bitmaps.isSet(miruInvertedIndex.getIndex(primitiveBuffer).get(), lastId + 2));
    }

    @Test(dataProvider = "miruInvertedIndexDataProviderWithData")
    public void testAndNot(MiruInvertedIndex<MutableRoaringBitmap> miruInvertedIndex, List<Integer> ids) throws Exception {
        byte[] primitiveBuffer = new byte[8];
        MiruBitmaps<MutableRoaringBitmap> bitmaps = new MiruBitmapsRoaringBuffer();

        MutableRoaringBitmap bitmap = new MutableRoaringBitmap();
        for (int i = 0; i < ids.size() / 2; i++) {
            bitmap.add(ids.get(i));
        }
        miruInvertedIndex.andNot(bitmap, primitiveBuffer);

        for (int i = 0; i < ids.size() / 2; i++) {
            assertFalse(bitmaps.isSet(miruInvertedIndex.getIndex(primitiveBuffer).get(), ids.get(i)));
        }
        for (int i = ids.size() / 2; i < ids.size(); i++) {
            assertTrue(bitmaps.isSet(miruInvertedIndex.getIndex(primitiveBuffer).get(), ids.get(i)));
        }
    }

    @Test(dataProvider = "miruInvertedIndexDataProviderWithData")
    public void testAndNotToSourceSize(MiruInvertedIndex<MutableRoaringBitmap> miruInvertedIndex, List<Integer> ids) throws Exception {
        byte[] primitiveBuffer = new byte[8];
        MiruBitmaps<MutableRoaringBitmap> bitmaps = new MiruBitmapsRoaringBuffer();

        MutableRoaringBitmap bitmap = new MutableRoaringBitmap();
        for (int i = 0; i < ids.size() / 2; i++) {
            bitmap.add(ids.get(i));
        }
        miruInvertedIndex.andNotToSourceSize(Collections.singletonList(bitmap), primitiveBuffer);

        for (int i = 0; i < ids.size() / 2; i++) {
            assertFalse(bitmaps.isSet(miruInvertedIndex.getIndex(primitiveBuffer).get(), ids.get(i)));
        }
        for (int i = ids.size() / 2; i < ids.size(); i++) {
            assertTrue(bitmaps.isSet(miruInvertedIndex.getIndex(primitiveBuffer).get(), ids.get(i)));
        }
    }

    @Test(dataProvider = "miruInvertedIndexDataProviderWithData")
    public void testOr(MiruInvertedIndex<MutableRoaringBitmap> miruInvertedIndex, List<Integer> ids) throws Exception {
        byte[] primitiveBuffer = new byte[8];
        MiruBitmaps<MutableRoaringBitmap> bitmaps = new MiruBitmapsRoaringBuffer();

        int lastId = ids.get(ids.size() - 1);

        miruInvertedIndex.append(primitiveBuffer, lastId + 2);

        MutableRoaringBitmap bitmap = new MutableRoaringBitmap();
        bitmap.add(lastId + 1);
        bitmap.add(lastId + 3);
        miruInvertedIndex.or(bitmap, primitiveBuffer);

        for (int id : ids) {
            assertTrue(bitmaps.isSet(miruInvertedIndex.getIndex(primitiveBuffer).get(), id));
        }
        assertTrue(bitmaps.isSet(miruInvertedIndex.getIndex(primitiveBuffer).get(), lastId + 1));
        assertTrue(bitmaps.isSet(miruInvertedIndex.getIndex(primitiveBuffer).get(), lastId + 2));
        assertTrue(bitmaps.isSet(miruInvertedIndex.getIndex(primitiveBuffer).get(), lastId + 3));
    }

    @Test(dataProvider = "miruInvertedIndexDataProviderWithData")
    public void testOrToSourceSize(MiruInvertedIndex<MutableRoaringBitmap> miruInvertedIndex, List<Integer> ids) throws Exception {
        byte[] primitiveBuffer = new byte[8];
        MiruBitmaps<MutableRoaringBitmap> bitmaps = new MiruBitmapsRoaringBuffer();

        int lastId = ids.get(ids.size() - 1);

        miruInvertedIndex.append(primitiveBuffer, lastId + 2);

        MutableRoaringBitmap bitmap = new MutableRoaringBitmap();
        bitmap.add(lastId + 1);
        bitmap.add(lastId + 3);
        miruInvertedIndex.orToSourceSize(bitmap, primitiveBuffer);

        for (int id : ids) {
            assertTrue(bitmaps.isSet(miruInvertedIndex.getIndex(primitiveBuffer).get(), id));
        }
        assertTrue(bitmaps.isSet(miruInvertedIndex.getIndex(primitiveBuffer).get(), lastId + 1));
        assertTrue(bitmaps.isSet(miruInvertedIndex.getIndex(primitiveBuffer).get(), lastId + 2));
        assertTrue(bitmaps.isSet(miruInvertedIndex.getIndex(primitiveBuffer).get(), lastId + 3)); // roaring ignores source size requirement
    }

    @DataProvider(name = "miruInvertedIndexDataProviderWithData")
    public Object[][] miruInvertedIndexDataProviderWithData() throws Exception {
        byte[] primitiveBuffer = new byte[8];
        MiruBitmapsRoaringBuffer bitmaps = new MiruBitmapsRoaringBuffer();

        MiruDeltaInvertedIndex<MutableRoaringBitmap> mergedIndex = buildDeltaInvertedIndex(bitmaps);
        mergedIndex.append(primitiveBuffer, 1, 2, 3, 4);
        mergedIndex.merge(primitiveBuffer);

        MiruDeltaInvertedIndex<MutableRoaringBitmap> unmergedIndex = buildDeltaInvertedIndex(bitmaps);
        unmergedIndex.append(primitiveBuffer, 1, 2, 3, 4);

        MiruDeltaInvertedIndex<MutableRoaringBitmap> partiallyMergedIndex = buildDeltaInvertedIndex(bitmaps);
        partiallyMergedIndex.append(primitiveBuffer, 1, 2);
        partiallyMergedIndex.merge(primitiveBuffer);
        partiallyMergedIndex.append(primitiveBuffer, 3, 4);

        return new Object[][] {
            { mergedIndex, Arrays.asList(1, 2, 3, 4) },
            { unmergedIndex, Arrays.asList(1, 2, 3, 4) },
            { partiallyMergedIndex, Arrays.asList(1, 2, 3, 4) }
        };
    }

    private <BM> MiruFilerInvertedIndex<BM> buildFilerInvertedIndex(MiruBitmaps<BM> bitmaps) throws Exception {
        return new MiruFilerInvertedIndex<>(bitmaps,
            new MiruFieldIndex.IndexKey(0, new byte[]{0}),
            IndexTestUtil.buildKeyedFilerStore("index",
                IndexTestUtil.buildByteBufferBackedChunkStores(4, new HeapByteBufferFactory(), 4_096)),
            -1,
            new Object());
    }

    private <BM> MiruDeltaInvertedIndex<BM> buildDeltaInvertedIndex(MiruBitmaps<BM> bitmaps) throws Exception {
        return new MiruDeltaInvertedIndex<>(bitmaps,
            buildFilerInvertedIndex(bitmaps),
            new MiruDeltaInvertedIndex.Delta<BM>(),
            new MiruFieldIndex.IndexKey(0, new byte[]{0}),
            null,
            null);
    }

    @Test(groups = "slow", enabled = false, description = "Concurrency test")
    public void testConcurrency() throws Exception {
        byte[] primitiveBuffer = new byte[8];
        MiruBitmapsRoaringBuffer bitmaps = new MiruBitmapsRoaringBuffer();
        ExecutorService executorService = Executors.newFixedThreadPool(8);

        final MiruFilerInvertedIndex<MutableRoaringBitmap> invertedIndex = buildFilerInvertedIndex(bitmaps);
        final AtomicInteger idProvider = new AtomicInteger();
        final AtomicInteger done = new AtomicInteger();
        final int runs = 10_000;
        final Random random = new Random();

        List<Future<?>> futures = Lists.newArrayListWithCapacity(8);

        futures.add(executorService.submit(() -> {
            while (done.get() < 7) {
                int id = idProvider.incrementAndGet();
                if (id == Integer.MAX_VALUE) {
                    System.out.println("appender hit max value");
                    break;
                }
                if (random.nextBoolean()) {
                    try {
                        invertedIndex.append(primitiveBuffer, id);
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                }
            }
            try {
                MutableRoaringBitmap index = invertedIndex.getIndex(primitiveBuffer).get();
                System.out.println("appender is done, final cardinality=" + index.getCardinality() + " bytes=" + index.getSizeInBytes());
            } catch (Exception e) {
                e.printStackTrace();
                throw new RuntimeException(e);
            }
        }));
        System.out.println("started appender");

        for (int i = 0; i < 7; i++) {
            futures.add(executorService.submit(() -> {
                MutableRoaringBitmap other = new MutableRoaringBitmap();
                int r = 0;
                for (int i1 = 0; i1 < runs; i1++) {
                    int size = idProvider.get();
                    while (r < size) {
                        if (random.nextBoolean()) {
                            other.add(r);
                        }
                        r++;
                    }

                    try {
                        MutableRoaringBitmap index = invertedIndex.getIndex(primitiveBuffer).get();
                        MutableRoaringBitmap container = new MutableRoaringBitmap();
                        RoaringBufferAggregation.and(container, index, other);
                    } catch (Exception e) {
                        done.incrementAndGet();
                        e.printStackTrace();
                        throw new RuntimeException(e);
                    }
                }

                System.out.println("aggregator is done, final cardinality=" + other.getCardinality() + " bytes=" + other.getSizeInBytes());
                done.incrementAndGet();
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
        byte[] primitiveBuffer = new byte[8];
        MiruFilerInvertedIndex<MutableRoaringBitmap> invertedIndex = buildFilerInvertedIndex(new MiruBitmapsRoaringBuffer());

        Random r = new Random(1_249_871_239_817_231_827l);
        long t = System.currentTimeMillis();
        for (int i = 0; i < 1_000_000; i++) {
            if (r.nextBoolean()) {
                invertedIndex.append(primitiveBuffer, i);
            }
        }

        long elapsed = System.currentTimeMillis() - t;
        MutableRoaringBitmap index = invertedIndex.getIndex(primitiveBuffer).get();
        System.out.println("cardinality=" + index.getCardinality() + " bytes=" + index.getSizeInBytes() + " elapsed=" + elapsed);
    }
}
