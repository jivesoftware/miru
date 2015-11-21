package com.jivesoftware.os.miru.service.index;

import com.google.common.collect.Lists;
import com.jivesoftware.os.filer.io.HeapByteBufferFactory;
import com.jivesoftware.os.filer.io.api.StackBuffer;
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
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;
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
    public void testSetId(MiruInvertedIndex<ImmutableRoaringBitmap> miruInvertedIndex, int appends, int sets) throws Exception {
        StackBuffer stackBuffer = new StackBuffer();
        Random r = new Random(1_234);
        int id = 0;
        int[] ids = new int[sets + appends];
        for (int i = 0; i < appends; i++) {
            id += 1 + (r.nextDouble() * 120_000);
            miruInvertedIndex.append(stackBuffer, id);
            ids[i] = id;
            if (i % 100_000 == 0) {
                System.out.println("add " + i);
            }
        }

        System.out.println("max id " + id);
        System.out.println("bitmap size " + miruInvertedIndex.getIndex(stackBuffer).get().getSizeInBytes());
        //Thread.sleep(2000);

        long timestamp = System.currentTimeMillis();
        long subTimestamp = System.currentTimeMillis();
        for (int i = 0; i < sets; i++) {
            miruInvertedIndex.remove(ids[i], stackBuffer);

            id += 1 + (r.nextDouble() * 120);
            miruInvertedIndex.append(stackBuffer, id);
            ids[appends + i] = id;

            if (i % 1_000 == 0) {
                //System.out.println("set " + i);
                System.out.println(String.format("set 1000, elapsed = %s, max id = %s, bitmap size = %s",
                    (System.currentTimeMillis() - subTimestamp), id, miruInvertedIndex.getIndex(stackBuffer).get().getSizeInBytes()));
                subTimestamp = System.currentTimeMillis();
            }
        }
        System.out.println("elapsed: " + (System.currentTimeMillis() - timestamp) + " ms");
    }

    @Test(dataProvider = "miruInvertedIndexDataProviderWithData")
    public void testAppend(MiruInvertedIndex<ImmutableRoaringBitmap> miruInvertedIndex, List<Integer> ids) throws Exception {
        StackBuffer stackBuffer = new StackBuffer();
        MiruBitmaps<MutableRoaringBitmap, ImmutableRoaringBitmap> bitmaps = new MiruBitmapsRoaringBuffer();

        int lastId = ids.get(ids.size() - 1);
        miruInvertedIndex.append(stackBuffer, lastId + 1);
        miruInvertedIndex.append(stackBuffer, lastId + 3);

        for (int id : ids) {
            assertTrue(bitmaps.isSet(miruInvertedIndex.getIndex(stackBuffer).get(), id));
        }

        assertTrue(bitmaps.isSet(miruInvertedIndex.getIndex(stackBuffer).get(), lastId + 1));
        assertFalse(bitmaps.isSet(miruInvertedIndex.getIndex(stackBuffer).get(), lastId + 2));
        assertTrue(bitmaps.isSet(miruInvertedIndex.getIndex(stackBuffer).get(), lastId + 3));
        assertFalse(bitmaps.isSet(miruInvertedIndex.getIndex(stackBuffer).get(), lastId + 4));
    }

    @Test(dataProvider = "miruInvertedIndexDataProviderWithData")
    public void testRemove(MiruInvertedIndex<ImmutableRoaringBitmap> miruInvertedIndex, List<Integer> ids) throws Exception {
        StackBuffer stackBuffer = new StackBuffer();
        ImmutableRoaringBitmap index = miruInvertedIndex.getIndex(stackBuffer).get();
        assertEquals(index.getCardinality(), ids.size());

        for (int id : ids) {
            assertTrue(index.contains(id));
        }

        for (int i = 0; i < ids.size() / 2; i++) {
            miruInvertedIndex.remove(ids.get(i), stackBuffer);
        }
        ids = ids.subList(ids.size() / 2, ids.size());

        index = miruInvertedIndex.getIndex(stackBuffer).get();
        assertEquals(index.getCardinality(), ids.size());
        for (int id : ids) {
            assertTrue(index.contains(id));
        }
    }

    @Test(dataProvider = "miruInvertedIndexDataProviderWithData")
    public void testSet(MiruInvertedIndex<ImmutableRoaringBitmap> miruInvertedIndex, List<Integer> ids) throws Exception {
        StackBuffer stackBuffer = new StackBuffer();
        MiruBitmaps<MutableRoaringBitmap, ImmutableRoaringBitmap> bitmaps = new MiruBitmapsRoaringBuffer();

        int lastId = ids.get(ids.size() - 1);

        miruInvertedIndex.append(stackBuffer, lastId + 2);
        miruInvertedIndex.set(stackBuffer, lastId + 1);

        for (int id : ids) {
            assertTrue(bitmaps.isSet(miruInvertedIndex.getIndex(stackBuffer).get(), id));
        }
        assertTrue(bitmaps.isSet(miruInvertedIndex.getIndex(stackBuffer).get(), lastId + 1));
        assertTrue(bitmaps.isSet(miruInvertedIndex.getIndex(stackBuffer).get(), lastId + 2));
    }

    @Test(dataProvider = "miruInvertedIndexDataProviderWithData")
    public void testSetNonIntermediateBit(MiruInvertedIndex<ImmutableRoaringBitmap> miruInvertedIndex, List<Integer> ids) throws Exception {
        StackBuffer stackBuffer = new StackBuffer();
        MiruBitmaps<MutableRoaringBitmap, ImmutableRoaringBitmap> bitmaps = new MiruBitmapsRoaringBuffer();

        int lastId = ids.get(ids.size() - 1);

        miruInvertedIndex.append(stackBuffer, lastId + 1);
        miruInvertedIndex.set(stackBuffer, lastId + 2);

        for (int id : ids) {
            assertTrue(bitmaps.isSet(miruInvertedIndex.getIndex(stackBuffer).get(), id));
        }
        assertTrue(bitmaps.isSet(miruInvertedIndex.getIndex(stackBuffer).get(), lastId + 1));
        assertTrue(bitmaps.isSet(miruInvertedIndex.getIndex(stackBuffer).get(), lastId + 2));
    }

    @Test(dataProvider = "miruInvertedIndexDataProviderWithData")
    public void testAndNot(MiruInvertedIndex<ImmutableRoaringBitmap> miruInvertedIndex, List<Integer> ids) throws Exception {
        StackBuffer stackBuffer = new StackBuffer();
        MiruBitmaps<MutableRoaringBitmap, ImmutableRoaringBitmap> bitmaps = new MiruBitmapsRoaringBuffer();

        MutableRoaringBitmap bitmap = new MutableRoaringBitmap();
        for (int i = 0; i < ids.size() / 2; i++) {
            bitmap.add(ids.get(i));
        }
        miruInvertedIndex.andNot(bitmap, stackBuffer);

        for (int i = 0; i < ids.size() / 2; i++) {
            assertFalse(bitmaps.isSet(miruInvertedIndex.getIndex(stackBuffer).get(), ids.get(i)));
        }
        for (int i = ids.size() / 2; i < ids.size(); i++) {
            assertTrue(bitmaps.isSet(miruInvertedIndex.getIndex(stackBuffer).get(), ids.get(i)));
        }
    }

    @Test(dataProvider = "miruInvertedIndexDataProviderWithData")
    public void testAndNotToSourceSize(MiruInvertedIndex<ImmutableRoaringBitmap> miruInvertedIndex, List<Integer> ids) throws Exception {
        StackBuffer stackBuffer = new StackBuffer();
        MiruBitmaps<MutableRoaringBitmap, ImmutableRoaringBitmap> bitmaps = new MiruBitmapsRoaringBuffer();

        MutableRoaringBitmap bitmap = new MutableRoaringBitmap();
        for (int i = 0; i < ids.size() / 2; i++) {
            bitmap.add(ids.get(i));
        }
        miruInvertedIndex.andNotToSourceSize(Collections.singletonList(bitmap), stackBuffer);

        for (int i = 0; i < ids.size() / 2; i++) {
            assertFalse(bitmaps.isSet(miruInvertedIndex.getIndex(stackBuffer).get(), ids.get(i)));
        }
        for (int i = ids.size() / 2; i < ids.size(); i++) {
            assertTrue(bitmaps.isSet(miruInvertedIndex.getIndex(stackBuffer).get(), ids.get(i)));
        }
    }

    @Test(dataProvider = "miruInvertedIndexDataProviderWithData")
    public void testOr(MiruInvertedIndex<ImmutableRoaringBitmap> miruInvertedIndex, List<Integer> ids) throws Exception {
        StackBuffer stackBuffer = new StackBuffer();
        MiruBitmaps<MutableRoaringBitmap, ImmutableRoaringBitmap> bitmaps = new MiruBitmapsRoaringBuffer();

        int lastId = ids.get(ids.size() - 1);

        miruInvertedIndex.append(stackBuffer, lastId + 2);

        MutableRoaringBitmap bitmap = new MutableRoaringBitmap();
        bitmap.add(lastId + 1);
        bitmap.add(lastId + 3);
        miruInvertedIndex.or(bitmap, stackBuffer);

        for (int id : ids) {
            assertTrue(bitmaps.isSet(miruInvertedIndex.getIndex(stackBuffer).get(), id));
        }
        assertTrue(bitmaps.isSet(miruInvertedIndex.getIndex(stackBuffer).get(), lastId + 1));
        assertTrue(bitmaps.isSet(miruInvertedIndex.getIndex(stackBuffer).get(), lastId + 2));
        assertTrue(bitmaps.isSet(miruInvertedIndex.getIndex(stackBuffer).get(), lastId + 3));
    }

    @Test(dataProvider = "miruInvertedIndexDataProviderWithData")
    public void testOrToSourceSize(MiruInvertedIndex<ImmutableRoaringBitmap> miruInvertedIndex, List<Integer> ids) throws Exception {
        StackBuffer stackBuffer = new StackBuffer();
        MiruBitmaps<MutableRoaringBitmap, ImmutableRoaringBitmap> bitmaps = new MiruBitmapsRoaringBuffer();

        int lastId = ids.get(ids.size() - 1);

        miruInvertedIndex.append(stackBuffer, lastId + 2);

        MutableRoaringBitmap bitmap = new MutableRoaringBitmap();
        bitmap.add(lastId + 1);
        bitmap.add(lastId + 3);
        miruInvertedIndex.orToSourceSize(bitmap, stackBuffer);

        for (int id : ids) {
            assertTrue(bitmaps.isSet(miruInvertedIndex.getIndex(stackBuffer).get(), id));
        }
        assertTrue(bitmaps.isSet(miruInvertedIndex.getIndex(stackBuffer).get(), lastId + 1));
        assertTrue(bitmaps.isSet(miruInvertedIndex.getIndex(stackBuffer).get(), lastId + 2));
        assertTrue(bitmaps.isSet(miruInvertedIndex.getIndex(stackBuffer).get(), lastId + 3)); // roaring ignores source size requirement
    }

    @DataProvider(name = "miruInvertedIndexDataProviderWithData")
    public Object[][] miruInvertedIndexDataProviderWithData() throws Exception {
        StackBuffer stackBuffer = new StackBuffer();
        MiruBitmapsRoaringBuffer bitmaps = new MiruBitmapsRoaringBuffer();

        MiruDeltaInvertedIndex<MutableRoaringBitmap, ImmutableRoaringBitmap> mergedIndex = buildDeltaInvertedIndex(bitmaps);
        mergedIndex.append(stackBuffer, 1, 2, 3, 4);
        mergedIndex.merge(stackBuffer);

        MiruDeltaInvertedIndex<MutableRoaringBitmap, ImmutableRoaringBitmap> unmergedIndex = buildDeltaInvertedIndex(bitmaps);
        unmergedIndex.append(stackBuffer, 1, 2, 3, 4);

        MiruDeltaInvertedIndex<MutableRoaringBitmap, ImmutableRoaringBitmap> partiallyMergedIndex = buildDeltaInvertedIndex(bitmaps);
        partiallyMergedIndex.append(stackBuffer, 1, 2);
        partiallyMergedIndex.merge(stackBuffer);
        partiallyMergedIndex.append(stackBuffer, 3, 4);

        return new Object[][] {
            { mergedIndex, Arrays.asList(1, 2, 3, 4) },
            { unmergedIndex, Arrays.asList(1, 2, 3, 4) },
            { partiallyMergedIndex, Arrays.asList(1, 2, 3, 4) }
        };
    }

    private <BM extends IBM, IBM> MiruFilerInvertedIndex<BM, IBM> buildFilerInvertedIndex(MiruBitmaps<BM, IBM> bitmaps) throws Exception {
        return new MiruFilerInvertedIndex<>(bitmaps,
            new MiruFieldIndex.IndexKey(0, new byte[]{0}),
            IndexTestUtil.buildKeyedFilerStore("index",
                IndexTestUtil.buildByteBufferBackedChunkStores(4, new HeapByteBufferFactory(), 4_096)),
            -1,
            new Object());
    }

    private <BM extends IBM, IBM> MiruDeltaInvertedIndex<BM, IBM> buildDeltaInvertedIndex(MiruBitmaps<BM, IBM> bitmaps) throws Exception {
        return new MiruDeltaInvertedIndex<>(bitmaps,
            buildFilerInvertedIndex(bitmaps),
            new MiruDeltaInvertedIndex.Delta<IBM>(),
            new MiruFieldIndex.IndexKey(0, new byte[]{0}),
            null,
            null);
    }

    @Test(groups = "slow", enabled = false, description = "Concurrency test")
    public void testConcurrency() throws Exception {
        StackBuffer stackBuffer = new StackBuffer();
        MiruBitmapsRoaringBuffer bitmaps = new MiruBitmapsRoaringBuffer();
        ExecutorService executorService = Executors.newFixedThreadPool(8);

        final MiruFilerInvertedIndex<MutableRoaringBitmap, ImmutableRoaringBitmap> invertedIndex = buildFilerInvertedIndex(bitmaps);
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
                        invertedIndex.append(stackBuffer, id);
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                }
            }
            try {
                ImmutableRoaringBitmap index = invertedIndex.getIndex(stackBuffer).get();
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
                        ImmutableRoaringBitmap index = invertedIndex.getIndex(stackBuffer).get();
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
        StackBuffer stackBuffer = new StackBuffer();
        MiruFilerInvertedIndex<MutableRoaringBitmap, ImmutableRoaringBitmap> invertedIndex = buildFilerInvertedIndex(new MiruBitmapsRoaringBuffer());

        Random r = new Random(1_249_871_239_817_231_827l);
        long t = System.currentTimeMillis();
        for (int i = 0; i < 1_000_000; i++) {
            if (r.nextBoolean()) {
                invertedIndex.append(stackBuffer, i);
            }
        }

        long elapsed = System.currentTimeMillis() - t;
        ImmutableRoaringBitmap index = invertedIndex.getIndex(stackBuffer).get();
        System.out.println("cardinality=" + index.getCardinality() + " bytes=" + index.getSizeInBytes() + " elapsed=" + elapsed);
    }
}
