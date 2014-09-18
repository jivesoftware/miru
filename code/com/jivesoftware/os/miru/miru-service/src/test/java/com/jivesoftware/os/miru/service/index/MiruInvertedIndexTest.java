package com.jivesoftware.os.miru.service.index;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.googlecode.javaewah.EWAHCompressedBitmap;
import com.jivesoftware.os.jive.utils.keyed.store.RandomAccessSwappableFiler;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.plugin.bitmap.MiruBitmaps;
import com.jivesoftware.os.miru.plugin.index.MiruInvertedIndex;
import com.jivesoftware.os.miru.service.bitmap.AnswerCardinalityLastSetBitmapStorage;
import com.jivesoftware.os.miru.service.bitmap.MiruBitmapsEWAH;
import com.jivesoftware.os.miru.service.index.disk.MiruOnDiskInvertedIndex;
import com.jivesoftware.os.miru.service.index.memory.MiruInMemoryInvertedIndex;
import java.io.File;
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
        System.out.println("bitmap size " + miruInvertedIndex.getIndex().sizeInBytes());
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
                        (System.currentTimeMillis() - subTimestamp), id, miruInvertedIndex.getIndex().sizeInBytes()));
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
        assertTrue(bitmaps.isSet(miruInvertedIndex.getIndex(), 1));
        assertTrue(bitmaps.isSet(miruInvertedIndex.getIndex(), 2));
        assertTrue(bitmaps.isSet(miruInvertedIndex.getIndex(), 3));
        assertTrue(bitmaps.isSet(miruInvertedIndex.getIndex(), 5));
        assertTrue(bitmaps.isSet(miruInvertedIndex.getIndex(), 8));
    }

    @Test(dataProvider = "miruInvertedIndexDataProviderWithData")
    public void testRemove(MiruInvertedIndex<EWAHCompressedBitmap> miruInvertedIndex, Set<Integer> ids) throws Exception {
        EWAHCompressedBitmap index = miruInvertedIndex.getIndex();
        assertEquals(index.cardinality(), ids.size());
        for (Integer id : ids) {
            assertTrue(index.get(id));
        }

        miruInvertedIndex.remove(2);
        ids.remove(2);

        index = miruInvertedIndex.getIndex();
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
        assertTrue(bitmaps.isSet(miruInvertedIndex.getIndex(), 1));
        assertTrue(bitmaps.isSet(miruInvertedIndex.getIndex(), 5));
        assertTrue(bitmaps.isSet(miruInvertedIndex.getIndex(), 3));
    }

    @Test(dataProvider = "miruInvertedIndexDataProvider")
    public void testSetNonIntermediateBit(MiruInvertedIndex<EWAHCompressedBitmap> miruInvertedIndex) throws Exception {
        MiruBitmaps<EWAHCompressedBitmap> bitmaps = new MiruBitmapsEWAH(100);
        miruInvertedIndex.append(1);
        miruInvertedIndex.set(2);

        assertTrue(bitmaps.isSet(miruInvertedIndex.getIndex(), 1));
        assertTrue(bitmaps.isSet(miruInvertedIndex.getIndex(), 2));
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
        miruInvertedIndex.andNotToSourceSize(bitmap);

        assertFalse(bitmaps.isSet(miruInvertedIndex.getIndex(), 1));
        assertTrue(bitmaps.isSet(miruInvertedIndex.getIndex(), 2));
        assertFalse(bitmaps.isSet(miruInvertedIndex.getIndex(), 3));
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

        assertTrue(bitmaps.isSet(miruInvertedIndex.getIndex(), 1));
        assertTrue(bitmaps.isSet(miruInvertedIndex.getIndex(), 2));
        assertTrue(bitmaps.isSet(miruInvertedIndex.getIndex(), 3));
        assertFalse(bitmaps.isSet(miruInvertedIndex.getIndex(), 4));
        assertFalse(bitmaps.isSet(miruInvertedIndex.getIndex(), 5)); // extra bit is ignored
    }

    @Test(dataProvider = "miruInvertedIndexDataProviderWithOverhead")
    public void testSizeInBytes(MiruInvertedIndex miruInvertedIndex, int overhead) throws Exception {
        System.out.println(miruInvertedIndex + " " + overhead);
        miruInvertedIndex.append(1);
        miruInvertedIndex.getIndex(); // force merge
        assertEquals(miruInvertedIndex.sizeInMemory() + miruInvertedIndex.sizeOnDisk(), 16 + overhead);
        miruInvertedIndex.append(100);
        miruInvertedIndex.getIndex(); // force merge
        assertEquals(miruInvertedIndex.sizeInMemory() + miruInvertedIndex.sizeOnDisk(), 24 + overhead);
        miruInvertedIndex.append(1_000);
        miruInvertedIndex.getIndex(); // force merge
        assertEquals(miruInvertedIndex.sizeInMemory() + miruInvertedIndex.sizeOnDisk(), 40 + overhead);
    }

    @DataProvider(name = "miruInvertedIndexDataProviderWithScale")
    public Object[][] miruInvertedIndexDataProviderWithScale() throws Exception {
        int memoryAppends = 1_000;
        int memorySets = 100_000;
        int diskAppends = 100;
        int diskSets = 1_000;

        return new Object[][] {
                { new MiruInMemoryInvertedIndex<>(new MiruBitmapsEWAH(100)), memoryAppends, memorySets },
                { new MiruOnDiskInvertedIndex<>(new MiruBitmapsEWAH(100), new RandomAccessSwappableFiler(File.createTempFile("inverted", "index"))),
                        diskAppends, diskSets }
        };
    }

    @DataProvider(name = "miruInvertedIndexDataProvider")
    public Object[][] miruInvertedIndexDataProvider() throws Exception {
        return new Object[][] {
                { new MiruInMemoryInvertedIndex<>(new MiruBitmapsEWAH(100)) },
                { new MiruOnDiskInvertedIndex<>(new MiruBitmapsEWAH(100), new RandomAccessSwappableFiler(File.createTempFile("inverted", "index"))) }
        };
    }

    @DataProvider(name = "miruInvertedIndexDataProviderWithOverhead")
    public Object[][] miruInvertedIndexDataProviderWithOverhead() throws Exception {
        return new Object[][] {
                { new MiruInMemoryInvertedIndex<>(new MiruBitmapsEWAH(4)), 0 },
                { new MiruOnDiskInvertedIndex<>(new MiruBitmapsEWAH(4), new RandomAccessSwappableFiler(File.createTempFile("inverted", "index"))), 12 }
                /**
                 * @see com.googlecode.javaewah.EWAHCompressedBitmap#serializedSizeInBytes()
                 */
        };
    }

    @DataProvider(name = "miruInvertedIndexDataProviderWithData")
    public Object[][] miruInvertedIndexDataProviderWithData() throws Exception {
        MiruTenantId tenantId = new MiruTenantId(new byte[] { 1 });
        MiruInMemoryInvertedIndex<EWAHCompressedBitmap> miruInMemoryInvertedIndex = new MiruInMemoryInvertedIndex<>(new MiruBitmapsEWAH(100));

        final EWAHCompressedBitmap bitmap = new EWAHCompressedBitmap();
        bitmap.set(1);
        bitmap.set(2);
        bitmap.set(3);
        miruInMemoryInvertedIndex.bulkImport(tenantId, new BulkExport<EWAHCompressedBitmap>() {
            @Override
            public EWAHCompressedBitmap bulkExport(MiruTenantId tenantId) throws Exception {
                return bitmap;
            }
        });

        MiruOnDiskInvertedIndex<EWAHCompressedBitmap> miruOnDiskInvertedIndex = new MiruOnDiskInvertedIndex<>(new MiruBitmapsEWAH(100),
                new RandomAccessSwappableFiler(File.createTempFile("inverted", "index")));
        miruOnDiskInvertedIndex.bulkImport(tenantId, miruInMemoryInvertedIndex);

        return new Object[][] {
                { miruInMemoryInvertedIndex, Sets.newHashSet(1, 2, 3) },
                { miruOnDiskInvertedIndex, Sets.newHashSet(1, 2, 3) }
        };
    }

    @Test(groups = "slow", enabled = false, description = "Concurrency test")
    public void testConcurrency() throws Exception {
        ExecutorService executorService = Executors.newFixedThreadPool(8);

        final MiruInMemoryInvertedIndex<EWAHCompressedBitmap> miruInMemoryInvertedIndex = new MiruInMemoryInvertedIndex<>(new MiruBitmapsEWAH(100));
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
                        miruInMemoryInvertedIndex.append(id);
                    }
                }
                try {
                    EWAHCompressedBitmap index = miruInMemoryInvertedIndex.getIndex();
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
                            EWAHCompressedBitmap index = miruInMemoryInvertedIndex.getIndex();
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
        MiruInMemoryInvertedIndex<EWAHCompressedBitmap> miruInMemoryInvertedIndex = new MiruInMemoryInvertedIndex<>(new MiruBitmapsEWAH(100));

        Random r = new Random(1_249_871_239_817_231_827l);
        long t = System.currentTimeMillis();
        for (int i = 0; i < 1_000_000; i++) {
            if (r.nextBoolean()) {
                miruInMemoryInvertedIndex.append(i);
            }
        }

        long elapsed = System.currentTimeMillis() - t;
        EWAHCompressedBitmap index = miruInMemoryInvertedIndex.getIndex();
        System.out.println("cardinality=" + index.cardinality() + " bits=" + index.sizeInBits() + " bytes=" + index.sizeInBytes() + " elapsed=" + elapsed);
    }
}
