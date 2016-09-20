package com.jivesoftware.os.miru.service.index;

import com.google.common.collect.Lists;
import com.jivesoftware.os.filer.io.api.StackBuffer;
import com.jivesoftware.os.jive.utils.ordered.id.ConstantWriterIdProvider;
import com.jivesoftware.os.jive.utils.ordered.id.JiveEpochTimestampProvider;
import com.jivesoftware.os.jive.utils.ordered.id.OrderIdProviderImpl;
import com.jivesoftware.os.jive.utils.ordered.id.SnowflakeIdPacker;
import com.jivesoftware.os.miru.bitmaps.roaring5.MiruBitmapsRoaring;
import com.jivesoftware.os.miru.plugin.bitmap.MiruBitmaps;
import com.jivesoftware.os.miru.plugin.index.BitmapAndLastId;
import com.jivesoftware.os.miru.plugin.index.MiruInvertedIndex;
import com.jivesoftware.os.miru.plugin.partition.TrackError;
import com.jivesoftware.os.miru.service.IndexTestUtil;
import com.jivesoftware.os.miru.service.index.lab.LabInvertedIndex;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import org.roaringbitmap.FastAggregation;
import org.roaringbitmap.RoaringBitmap;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static com.jivesoftware.os.miru.service.IndexTestUtil.getIndex;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

/**
 *
 */
public class MiruInvertedIndexTest {

    @Test(dataProvider = "miruInvertedIndexDataProviderWithData",
        groups = "slow", enabled = false, description = "Performance test")
    public void testSetId(MiruInvertedIndex<RoaringBitmap, RoaringBitmap> miruInvertedIndex, int appends, int sets) throws Exception {
        StackBuffer stackBuffer = new StackBuffer();
        Random r = new Random(1_234);
        int id = 0;
        int[] ids = new int[sets + appends];
        for (int i = 0; i < appends; i++) {
            id += 1 + (r.nextDouble() * 120_000);
            miruInvertedIndex.set(stackBuffer, id);
            ids[i] = id;
            if (i % 100_000 == 0) {
                System.out.println("add " + i);
            }
        }

        System.out.println("max id " + id);
        System.out.println("bitmap size " + getIndex(miruInvertedIndex, stackBuffer).getBitmap().getSizeInBytes());
        //Thread.sleep(2000);

        long timestamp = System.currentTimeMillis();
        long subTimestamp = System.currentTimeMillis();
        for (int i = 0; i < sets; i++) {
            miruInvertedIndex.remove(stackBuffer, ids[i]);

            id += 1 + (r.nextDouble() * 120);
            miruInvertedIndex.set(stackBuffer, id);
            ids[appends + i] = id;

            if (i % 1_000 == 0) {
                //System.out.println("set " + i);
                System.out.println(String.format("set 1000, elapsed = %s, max id = %s, bitmap size = %s",
                    (System.currentTimeMillis() - subTimestamp), id, getIndex(miruInvertedIndex, stackBuffer).getBitmap().getSizeInBytes()));
                subTimestamp = System.currentTimeMillis();
            }
        }
        System.out.println("elapsed: " + (System.currentTimeMillis() - timestamp) + " ms");
    }

    @Test(dataProvider = "miruInvertedIndexDataProviderWithData")
    public void testAppend(MiruInvertedIndex<RoaringBitmap, RoaringBitmap> miruInvertedIndex, List<Integer> ids) throws Exception {
        StackBuffer stackBuffer = new StackBuffer();
        MiruBitmaps<RoaringBitmap, RoaringBitmap> bitmaps = new MiruBitmapsRoaring();

        int lastId = ids.get(ids.size() - 1);
        miruInvertedIndex.set(stackBuffer, lastId + 1);
        miruInvertedIndex.set(stackBuffer, lastId + 3);

        for (int id : ids) {
            assertTrue(bitmaps.isSet(getIndex(miruInvertedIndex, stackBuffer).getBitmap(), id));
        }

        assertTrue(bitmaps.isSet(getIndex(miruInvertedIndex, stackBuffer).getBitmap(), lastId + 1));
        assertFalse(bitmaps.isSet(getIndex(miruInvertedIndex, stackBuffer).getBitmap(), lastId + 2));
        assertTrue(bitmaps.isSet(getIndex(miruInvertedIndex, stackBuffer).getBitmap(), lastId + 3));
        assertFalse(bitmaps.isSet(getIndex(miruInvertedIndex, stackBuffer).getBitmap(), lastId + 4));
    }

    @Test(dataProvider = "miruInvertedIndexDataProviderWithData")
    public void testRemove(MiruInvertedIndex<RoaringBitmap, RoaringBitmap> miruInvertedIndex, List<Integer> ids) throws Exception {
        StackBuffer stackBuffer = new StackBuffer();
        RoaringBitmap index = getIndex(miruInvertedIndex, stackBuffer).getBitmap();
        assertEquals(index.getCardinality(), ids.size());

        for (int id : ids) {
            assertTrue(index.contains(id));
        }

        for (int i = 0; i < ids.size() / 2; i++) {
            miruInvertedIndex.remove(stackBuffer, ids.get(i));
        }
        ids = ids.subList(ids.size() / 2, ids.size());

        index = getIndex(miruInvertedIndex, stackBuffer).getBitmap();
        assertEquals(index.getCardinality(), ids.size());
        for (int id : ids) {
            assertTrue(index.contains(id));
        }
    }

    @Test(dataProvider = "miruInvertedIndexDataProviderWithData")
    public void testSet(MiruInvertedIndex<RoaringBitmap, RoaringBitmap> miruInvertedIndex, List<Integer> ids) throws Exception {
        StackBuffer stackBuffer = new StackBuffer();
        MiruBitmaps<RoaringBitmap, RoaringBitmap> bitmaps = new MiruBitmapsRoaring();

        int lastId = ids.get(ids.size() - 1);

        miruInvertedIndex.set(stackBuffer, lastId + 2);
        miruInvertedIndex.set(stackBuffer, lastId + 1);

        for (int id : ids) {
            assertTrue(bitmaps.isSet(getIndex(miruInvertedIndex, stackBuffer).getBitmap(), id));
        }
        assertTrue(bitmaps.isSet(getIndex(miruInvertedIndex, stackBuffer).getBitmap(), lastId + 1));
        assertTrue(bitmaps.isSet(getIndex(miruInvertedIndex, stackBuffer).getBitmap(), lastId + 2));
    }

    @Test(dataProvider = "miruInvertedIndexDataProviderWithData")
    public void testSetNonIntermediateBit(MiruInvertedIndex<RoaringBitmap, RoaringBitmap> miruInvertedIndex,
        List<Integer> ids) throws Exception {
        StackBuffer stackBuffer = new StackBuffer();
        MiruBitmaps<RoaringBitmap, RoaringBitmap> bitmaps = new MiruBitmapsRoaring();

        int lastId = ids.get(ids.size() - 1);

        miruInvertedIndex.set(stackBuffer, lastId + 1);
        miruInvertedIndex.set(stackBuffer, lastId + 2);

        for (int id : ids) {
            assertTrue(bitmaps.isSet(getIndex(miruInvertedIndex, stackBuffer).getBitmap(), id));
        }
        assertTrue(bitmaps.isSet(getIndex(miruInvertedIndex, stackBuffer).getBitmap(), lastId + 1));
        assertTrue(bitmaps.isSet(getIndex(miruInvertedIndex, stackBuffer).getBitmap(), lastId + 2));
    }

    @Test(dataProvider = "miruInvertedIndexDataProviderWithData")
    public void testAndNot(MiruInvertedIndex<RoaringBitmap, RoaringBitmap> miruInvertedIndex, List<Integer> ids) throws Exception {
        StackBuffer stackBuffer = new StackBuffer();
        MiruBitmaps<RoaringBitmap, RoaringBitmap> bitmaps = new MiruBitmapsRoaring();

        RoaringBitmap bitmap = new RoaringBitmap();
        for (int i = 0; i < ids.size() / 2; i++) {
            bitmap.add(ids.get(i));
        }
        miruInvertedIndex.andNot(bitmap, stackBuffer);

        for (int i = 0; i < ids.size() / 2; i++) {
            RoaringBitmap got = getIndex(miruInvertedIndex, stackBuffer).getBitmap();
            assertFalse(bitmaps.isSet(got, ids.get(i)), "Mismatch at " + i);
        }
        for (int i = ids.size() / 2; i < ids.size(); i++) {
            RoaringBitmap got = getIndex(miruInvertedIndex, stackBuffer).getBitmap();
            assertTrue(bitmaps.isSet(got, ids.get(i)), "Mismatch at " + i);
        }
    }

    @Test(dataProvider = "miruInvertedIndexDataProviderWithData")
    public void testAndNotToSourceSize(MiruInvertedIndex<RoaringBitmap, RoaringBitmap> miruInvertedIndex, List<Integer> ids) throws Exception {
        StackBuffer stackBuffer = new StackBuffer();
        MiruBitmaps<RoaringBitmap, RoaringBitmap> bitmaps = new MiruBitmapsRoaring();

        RoaringBitmap bitmap = new RoaringBitmap();
        for (int i = 0; i < ids.size() / 2; i++) {
            bitmap.add(ids.get(i));
        }
        miruInvertedIndex.andNotToSourceSize(Collections.singletonList(bitmap), stackBuffer);

        for (int i = 0; i < ids.size() / 2; i++) {
            assertFalse(bitmaps.isSet(getIndex(miruInvertedIndex, stackBuffer).getBitmap(), ids.get(i)));
        }
        for (int i = ids.size() / 2; i < ids.size(); i++) {
            assertTrue(bitmaps.isSet(getIndex(miruInvertedIndex, stackBuffer).getBitmap(), ids.get(i)));
        }
    }

    @Test(dataProvider = "miruInvertedIndexDataProviderWithData")
    public void testOr(MiruInvertedIndex<RoaringBitmap, RoaringBitmap> miruInvertedIndex, List<Integer> ids) throws Exception {
        StackBuffer stackBuffer = new StackBuffer();
        MiruBitmaps<RoaringBitmap, RoaringBitmap> bitmaps = new MiruBitmapsRoaring();

        int lastId = ids.get(ids.size() - 1);

        miruInvertedIndex.set(stackBuffer, lastId + 2);

        RoaringBitmap bitmap = new RoaringBitmap();
        bitmap.add(lastId + 1);
        bitmap.add(lastId + 3);
        miruInvertedIndex.or(bitmap, stackBuffer);

        for (int id : ids) {
            assertTrue(bitmaps.isSet(getIndex(miruInvertedIndex, stackBuffer).getBitmap(), id));
        }
        assertTrue(bitmaps.isSet(getIndex(miruInvertedIndex, stackBuffer).getBitmap(), lastId + 1));
        assertTrue(bitmaps.isSet(getIndex(miruInvertedIndex, stackBuffer).getBitmap(), lastId + 2));
        assertTrue(bitmaps.isSet(getIndex(miruInvertedIndex, stackBuffer).getBitmap(), lastId + 3));
    }

    @Test(dataProvider = "miruInvertedIndexDataProviderWithData")
    public void testOrToSourceSize(MiruInvertedIndex<RoaringBitmap, RoaringBitmap> miruInvertedIndex, List<Integer> ids) throws Exception {
        StackBuffer stackBuffer = new StackBuffer();
        MiruBitmaps<RoaringBitmap, RoaringBitmap> bitmaps = new MiruBitmapsRoaring();

        int lastId = ids.get(ids.size() - 1);

        miruInvertedIndex.set(stackBuffer, lastId + 2);

        RoaringBitmap bitmap = new RoaringBitmap();
        bitmap.add(lastId + 1);
        bitmap.add(lastId + 3);
        miruInvertedIndex.orToSourceSize(bitmap, stackBuffer);

        for (int id : ids) {
            assertTrue(bitmaps.isSet(getIndex(miruInvertedIndex, stackBuffer).getBitmap(), id));
        }
        assertTrue(bitmaps.isSet(getIndex(miruInvertedIndex, stackBuffer).getBitmap(), lastId + 1));
        assertTrue(bitmaps.isSet(getIndex(miruInvertedIndex, stackBuffer).getBitmap(), lastId + 2));
        assertTrue(bitmaps.isSet(getIndex(miruInvertedIndex, stackBuffer).getBitmap(), lastId + 3)); // roaring ignores source size requirement
    }

    @Test
    public void testSetIfEmpty() throws Exception {
        StackBuffer stackBuffer = new StackBuffer();
        //MiruSchema schema = new Builder("test", 1).build();
        MiruBitmapsRoaring bitmaps = new MiruBitmapsRoaring();

        for (boolean atomized : new boolean[] { false, true }) {
            MiruInvertedIndex<RoaringBitmap, RoaringBitmap> index = buildInvertedIndex(atomized, bitmaps);

            // setIfEmpty index 1
            index.setIfEmpty(stackBuffer, 1);
            RoaringBitmap got = getIndex(index, stackBuffer).getBitmap();
            assertEquals(got.getCardinality(), 1);
            assertTrue(got.contains(1));

            // setIfEmpty index 2 noops
            index.setIfEmpty(stackBuffer, 2);
            got = getIndex(index, stackBuffer).getBitmap();
            assertEquals(got.getCardinality(), 1);
            assertTrue(got.contains(1));
            assertFalse(got.contains(2));

            // check index 1 is still set after merge
            got = getIndex(index, stackBuffer).getBitmap();
            assertEquals(got.getCardinality(), 1);
            assertTrue(got.contains(1));

            // setIfEmpty index 3 noops
            index.setIfEmpty(stackBuffer, 3);
            got = getIndex(index, stackBuffer).getBitmap();
            assertEquals(got.getCardinality(), 1);
            assertTrue(got.contains(1));
            assertFalse(got.contains(3));

            // set index 4
            index.set(stackBuffer, 4);
            got = getIndex(index, stackBuffer).getBitmap();
            assertEquals(got.getCardinality(), 2);
            assertTrue(got.contains(1));
            assertTrue(got.contains(4));

            // remove index 1, 4
            index.remove(stackBuffer, 1);
            index.remove(stackBuffer, 4);
            if (atomized) {
                assertFalse(getIndex(index, stackBuffer).isSet());
            } else {
                got = getIndex(index, stackBuffer).getBitmap();
                assertEquals(got.getCardinality(), 0);
            }

            // setIfEmpty index 5
            index.setIfEmpty(stackBuffer, 5);
            got = getIndex(index, stackBuffer).getBitmap();
            assertEquals(got.getCardinality(), 1);

            // setIfEmpty index 6 noops
            index.setIfEmpty(stackBuffer, 6);
            got = getIndex(index, stackBuffer).getBitmap();
            assertEquals(got.getCardinality(), 1);
        }
    }

    @DataProvider(name = "miruInvertedIndexDataProviderWithData")
    public Object[][] miruInvertedIndexDataProviderWithData() throws Exception {
        StackBuffer stackBuffer = new StackBuffer();
        //MiruSchema schema = new Builder("test", 1).build();
        MiruBitmapsRoaring bitmaps = new MiruBitmapsRoaring();

        MiruInvertedIndex<RoaringBitmap, RoaringBitmap> mergedIndex = buildInvertedIndex(false, bitmaps);
        mergedIndex.set(stackBuffer, 1, 2, 3, 4);

        MiruInvertedIndex<RoaringBitmap, RoaringBitmap> atomizedIndex = buildInvertedIndex(true, bitmaps);
        atomizedIndex.set(stackBuffer, 1, 2, 3, 4);

        return new Object[][] {
            { mergedIndex, Arrays.asList(1, 2, 3, 4) },
            { atomizedIndex, Arrays.asList(1, 2, 3, 4) }
        };
    }

    private <BM extends IBM, IBM> MiruInvertedIndex<BM, IBM> buildInvertedIndex(boolean atomized, MiruBitmaps<BM, IBM> bitmaps) throws Exception {
        return new LabInvertedIndex<>(
            new OrderIdProviderImpl(new ConstantWriterIdProvider(1), new SnowflakeIdPacker(), new JiveEpochTimestampProvider()),
            bitmaps,
            new TrackError() {
                @Override
                public void error(String reason) {
                }

                @Override
                public void reset() {
                }
            },
            "test",
            0,
            atomized,
            new byte[] { 0 },
            IndexTestUtil.buildValueIndex("bitmap"),
            new byte[] { 0 },
            IndexTestUtil.buildValueIndex("term"),
            new Object());
    }

    @Test(groups = "slow", enabled = false, description = "Concurrency test")
    public void testConcurrency() throws Exception {
        StackBuffer stackBuffer = new StackBuffer();
        MiruBitmapsRoaring bitmaps = new MiruBitmapsRoaring();
        ExecutorService executorService = Executors.newFixedThreadPool(8);

        final MiruInvertedIndex<RoaringBitmap, RoaringBitmap> invertedIndex = buildInvertedIndex(false, bitmaps);
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
                        invertedIndex.set(stackBuffer, id);
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                }
            }
            try {
                RoaringBitmap index = getIndex(invertedIndex, stackBuffer).getBitmap();
                System.out.println("appender is done, final cardinality=" + index.getCardinality() + " bytes=" + index.getSizeInBytes());
            } catch (Exception e) {
                e.printStackTrace();
                throw new RuntimeException(e);
            }
        }));
        System.out.println("started appender");

        for (int i = 0; i < 7; i++) {
            futures.add(executorService.submit(() -> {
                RoaringBitmap other = new RoaringBitmap();
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
                        RoaringBitmap index = getIndex(invertedIndex, stackBuffer).getBitmap();
                        RoaringBitmap container = FastAggregation.and(index, other);
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
        MiruInvertedIndex<RoaringBitmap, RoaringBitmap> invertedIndex = buildInvertedIndex(false, new MiruBitmapsRoaring());

        Random r = new Random();
        long t = System.currentTimeMillis();
        for (int i = 0; i < 1_000_000; i++) {
            if (r.nextBoolean()) {
                invertedIndex.set(stackBuffer, i);
            }
        }

        long elapsed = System.currentTimeMillis() - t;
        RoaringBitmap index = getIndex(invertedIndex, stackBuffer).getBitmap();
        System.out.println("cardinality=" + index.getCardinality() + " bytes=" + index.getSizeInBytes() + " elapsed=" + elapsed);
    }
}
