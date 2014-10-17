package com.jivesoftware.os.miru.service.index.memory;

import com.google.common.collect.Lists;
import com.jivesoftware.os.filer.io.FilerIO;
import com.jivesoftware.os.filer.io.HeapByteBufferFactory;
import com.jivesoftware.os.filer.map.store.BytesObjectMapStore;
import com.jivesoftware.os.miru.service.index.IndexKeyFunction;
import gnu.trove.impl.Constants;
import gnu.trove.impl.hash.TPrimitiveHash;
import gnu.trove.map.hash.TLongIntHashMap;
import gnu.trove.map.hash.TLongObjectHashMap;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class MiruInMemoryIndexTest {

    @Test(enabled = false)
    public void testTrove() throws Exception {
        final IndexKeyFunction indexKeyFunction = new IndexKeyFunction();
        final int numIterations = 20;
        final int numFields = 10;
        final int numTerms = 100_000;
        int[] values = new int[numFields * numTerms];
        for (int i = 0; i < values.length; i++) {
            values[i] = i;
        }

        for (int i = 0; i < numIterations; i++) {
            System.out.println("---------------------- " + i + " ----------------------");

            // trove setup
            TLongIntHashMap trove = new TLongIntHashMap(10, Constants.DEFAULT_LOAD_FACTOR, -1, -1);

            // trove insert
            long start = System.currentTimeMillis();
            for (int fieldId = 0; fieldId < numFields; fieldId++) {
                for (int termId = 0; termId < numTerms; termId++) {
                    long key = indexKeyFunction.getKey(fieldId, termId);
                    trove.put(key, values[fieldId * numFields + termId]);
                }
            }
            System.out.println("Trove: Inserted " + trove.size() + " in " + (System.currentTimeMillis() - start) + "ms");

            // trove retrieve
            start = System.currentTimeMillis();
            for (int fieldId = 0; fieldId < numFields; fieldId++) {
                for (int termId = 0; termId < numTerms; termId++) {
                    long key = indexKeyFunction.getKey(fieldId, termId);
                    int retrieved = trove.get(key);
                    assertEquals(retrieved, values[fieldId * numFields + termId], "Failed at " + fieldId + ", " + termId);
                }
            }

            if (i == numIterations - 1) {
                Thread.sleep(600000);
            }

            System.out.println("Trove: Retrieved " + trove.size() + " in " + (System.currentTimeMillis() - start) + "ms");
        }
    }

    @Test(enabled = false)
    public void testMapComparisons() throws Exception {
        final IndexKeyFunction indexKeyFunction = new IndexKeyFunction();
        final int numIterations = 100;
        final int numFields = 10;
        final int numTerms = 100_000;
        //float loadFactor = Constants.DEFAULT_LOAD_FACTOR;
        final int initialCapacity = 10; //(int) ((float) (numFields * numTerms) / loadFactor);
        Object[] obj = new Object[numFields * numTerms];
        for (int i = 0; i < obj.length; i++) {
            obj[i] = i;
        }

        for (int i = 0; i < numIterations; i++) {
            System.out.println("---------------------- " + i + " ----------------------");

            // trove setup
            TLongObjectHashMap<Object> trove = new TLongObjectHashMap<>(
                numFields * numTerms * 2, Constants.DEFAULT_LOAD_FACTOR, -1);

            // trove insert
            long start = System.currentTimeMillis();
            for (int fieldId = 0; fieldId < numFields; fieldId++) {
                for (int termId = 0; termId < numTerms; termId++) {
                    long key = indexKeyFunction.getKey(fieldId, termId);
                    trove.put(key, obj[fieldId * numFields + termId]);
                }
            }
            System.out.println("Trove: Inserted " + trove.size() + " in " + (System.currentTimeMillis() - start) + "ms");

            // trove retrieve
            start = System.currentTimeMillis();
            for (int fieldId = 0; fieldId < numFields; fieldId++) {
                for (int termId = 0; termId < numTerms; termId++) {
                    long key = indexKeyFunction.getKey(fieldId, termId);
                    Object retrieved = trove.get(key);
                    assertTrue(retrieved == obj[fieldId * numFields + termId], "Failed at " + fieldId + ", " + termId);
                }
            }
            System.out.println("Trove: Retrieved " + trove.size() + " in " + (System.currentTimeMillis() - start) + "ms");

            // bytebuffer mapstore setup
            BytesObjectMapStore<Long, Object> byteBufferMapStore =
                new BytesObjectMapStore<Long, Object>(8, numFields * numTerms * 2, null, new HeapByteBufferFactory()) {
                    @Override
                    public byte[] keyBytes(Long key) {
                        return FilerIO.longBytes(key);
                    }

                    @Override
                    public Long bytesKey(byte[] bytes, int offset) {
                        return FilerIO.bytesLong(bytes, offset);
                    }
                };

            // bytebuffer mapstore insert
            start = System.currentTimeMillis();
            for (int fieldId = 0; fieldId < numFields; fieldId++) {
                for (int termId = 0; termId < numTerms; termId++) {
                    long key = indexKeyFunction.getKey(fieldId, termId);
                    byteBufferMapStore.add(key, obj[fieldId * numFields + termId]);
                }
            }
            System.out.println("ByteBufferMapStore: Inserted " + numTerms * numFields + " in " +
                (System.currentTimeMillis() - start) + "ms");

            // bytebuffer mapstore retrieve
            start = System.currentTimeMillis();
            for (int fieldId = 0; fieldId < numFields; fieldId++) {
                for (int termId = 0; termId < numTerms; termId++) {
                    long key = indexKeyFunction.getKey(fieldId, termId);
                    Object retrieved = byteBufferMapStore.getUnsafe(key);
                    assertTrue(retrieved == obj[fieldId * numFields + termId], "Failed at " + fieldId + ", " + termId);
                }
            }
            System.out.println("ByteBufferMapStore: Retrieved " + numTerms * numFields + " in " +
                (System.currentTimeMillis() - start) + "ms");
        }
    }

    @Test(enabled = false)
    public void testKeyHash() {
        List<Strategy> strategies = Lists.newArrayList(
            /*
            new Strategy() {
                @Override
                public String name() {
                    return "original";
                }

                @Override
                public long getKey(int fieldId, int termId) {
                    return FilerIO.bytesLong(FilerIO.intArrayToByteArray(new int[] { fieldId, termId }));
                }
            },
            */
            new Strategy() {
                @Override
                public String name() {
                    return "reverse";
                }

                @Override
                public long getKey(int fieldId, int termId) {
                    return Long.reverse((((long) termId) << 32) | fieldId);
                }
            },
            /*
            new Strategy() {
                @Override
                public String name() {
                    return "balls12345";
                }

                @Override
                public long getKey(int fieldId, int termId) {
                    byte[] array = FilerIO.intArrayToByteArray(new int[] { fieldId, termId });
                    TByteArrayList arrayList = new TByteArrayList(array);
                    Random rgen = new Random(12345);
                    arrayList.shuffle(rgen);
                    return FilerIO.bytesLong(arrayList.toArray());
                }
            },
            */
            new Strategy() {
                @Override
                public String name() {
                    return "random";
                }

                @Override
                public long getKey(int fieldId, int termId) {
                    return new Random(fieldId).nextLong() ^ new Random(termId).nextLong();
                }
            }
        );

        for (Strategy strategy : strategies) {
            runStrategy(strategy);
        }
    }

    private static void runStrategy(Strategy strategy) {
        Object object = new Object();
        int noEntryKey = -1;

        for (float loadFactor : new float[] { 0.1f, 0.3f, 0.5f, 0.7f, 0.9f }) {
            TLongObjectHashMap<Object> index = new TLongObjectHashMap<>(Constants.DEFAULT_CAPACITY, loadFactor, noEntryKey);

            int rounds = 5_000;
            for (int i = 0; i < rounds; i++) {
                for (int fieldId = 0; fieldId < 20; fieldId++) {
                    int termId = i * 20;
                    long fieldTermId = strategy.getKey(fieldId, termId);
                    index.put(fieldTermId, object);
                }
            }

            byte[] states = index._states;
            int runLength = 0;
            int[] histo = new int[10];
            for (long state : states) {
                if (state == TPrimitiveHash.FULL) {
                    runLength++;
                } else {
                    if (runLength > 0) {
                        //System.out.println(runLength);
                        histo[(int) (Math.log(runLength) / Math.log(2))]++;
                        runLength = 0;
                    }
                }
            }

            System.out.println("------------------------");
            System.out.println("lf: " + loadFactor + ", size: " + states.length);
            System.out.println(Arrays.toString(histo) + "\t" + strategy.name());
        }
    }

    private interface Strategy {

        String name();

        long getKey(int fieldId, int termId);
    }
}