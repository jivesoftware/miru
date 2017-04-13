package org.roaringbitmap;

import com.jivesoftware.os.miru.plugin.index.BitmapAndLastId;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.util.Random;
import org.testng.Assert;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;

public class RoaringInspectionTest {

    @Test
    public void testOptimize() throws Exception {
        RoaringBitmap bitmap = new RoaringBitmap();
        Random r = new Random(123);
        for (int i = 0; i < 3_000_000; i += 65_536) {
            if (r.nextBoolean()) {
                for (int j = 0; j < 65_536; j++) {
                    bitmap.add(i + j);
                }
            }
        }

        int initialSize = bitmap.serializedSizeInBytes();
        int[] keys = RoaringInspection.keys(bitmap);
        RoaringInspection.optimize(bitmap, keys);
        Assert.assertTrue(bitmap.serializedSizeInBytes() < initialSize);
    }

    @Test
    public void testSplitJoin() throws Exception {
        RoaringBitmap bitmap = new RoaringBitmap();
        Random r = new Random(123);
        for (int i = 0; i < 3_000_000; i++) {
            if (r.nextBoolean()) {
                bitmap.add(i);
            }
        }

        RoaringBitmap[] split = RoaringInspection.split(bitmap);
        RoaringBitmap joined = RoaringInspection.join(split);

        assertEquals(joined, bitmap);
    }

    @Test
    public void testExtract() throws Exception {
        RoaringBitmap bitmap1 = new RoaringBitmap();
        Random r = new Random(123);
        for (int i = 0; i < 3_000_000; i += 65_536) {
            if (r.nextBoolean()) {
                bitmap1.add(i);
            }
        }

        RoaringBitmap bitmap2 = new RoaringBitmap();
        bitmap2.flip(0, 3_000_000);

        int[] keys1 = RoaringInspection.keys(bitmap1);

        RoaringBitmap[] extracted = RoaringInspection.extract(bitmap2, keys1);
        RoaringBitmap joined = RoaringInspection.join(extracted);

        int[] keys2 = RoaringInspection.keys(joined);

        assertEquals(keys1, keys2);
    }

    @Test
    public void testSizeInBits() throws Exception {
        RoaringBitmap bitmap = new RoaringBitmap();

        assertEquals(RoaringInspection.sizeInBits(bitmap), 0);
        bitmap.add(0);
        assertEquals(RoaringInspection.sizeInBits(bitmap), 1 << 16);
        bitmap.add(1 << 16 - 1);
        assertEquals(RoaringInspection.sizeInBits(bitmap), 1 << 16);
        bitmap.add(1 << 16);
        assertEquals(RoaringInspection.sizeInBits(bitmap), 2 << 16);
        bitmap.add(2 << 16 - 1);
        assertEquals(RoaringInspection.sizeInBits(bitmap), 2 << 16);
        bitmap.add(2 << 16);
        assertEquals(RoaringInspection.sizeInBits(bitmap), 3 << 16);
    }

    @Test
    public void testCardinalityInBuckets_dense_uncontained() throws Exception {
        RoaringBitmap bitmap = new RoaringBitmap();
        for (int i = 0; i < 100_001; i++) {
            bitmap.add(i);
        }
        int[][] indexes = new int[][] {
            { 0, 10_000, 20_000, 30_000, 40_000, 50_000, 60_000, 70_000, 80_000, 90_000, 100_000 },
            { 1, 10_001, 20_001, 30_001, 40_001, 50_001, 60_001, 70_001, 80_001, 90_001, 100_001 } };
        long[][] cardinalityInBuckets = new long[2][indexes[0].length - 1];
        RoaringInspection.cardinalityInBuckets(bitmap, indexes, cardinalityInBuckets);
        for (int i = 0; i < 2; i++) {
            assertEquals(cardinalityInBuckets[i].length, 10);
            for (long cardinalityInBucket : cardinalityInBuckets[i]) {
                assertEquals(cardinalityInBucket, 10_000);
            }
        }
    }

    @Test
    public void testCardinalityInBuckets_sparse_uncontained() throws Exception {
        RoaringBitmap bitmap = new RoaringBitmap();
        for (int i = 0; i < 100_001; i += 100) {
            bitmap.add(i);
        }
        int[][] indexes = new int[][] {
            { 0, 10_000, 20_000, 30_000, 40_000, 50_000, 60_000, 70_000, 80_000, 90_000, 100_000 },
            { 1, 10_001, 20_001, 30_001, 40_001, 50_001, 60_001, 70_001, 80_001, 90_001, 100_001 } };
        long[][] cardinalityInBuckets = new long[2][indexes[0].length - 1];
        RoaringInspection.cardinalityInBuckets(bitmap, indexes, cardinalityInBuckets);
        for (int i = 0; i < 2; i++) {
            assertEquals(cardinalityInBuckets[i].length, 10);
            for (long cardinalityInBucket : cardinalityInBuckets[i]) {
                assertEquals(cardinalityInBucket, 100);
            }
        }
    }

    @Test
    public void testCardinalityInBuckets_dense_contained() throws Exception {
        RoaringBitmap bitmap = new RoaringBitmap();
        for (int i = 0; i < 131_073; i++) {
            bitmap.add(i);
        }
        int[][] indexes = new int[][] {
            { 0, 65_536, 131_072 },
            { 1, 65_537, 131_073 } };
        long[][] cardinalityInBuckets = new long[2][indexes[0].length - 1];
        RoaringInspection.cardinalityInBuckets(bitmap, indexes, cardinalityInBuckets);
        for (int i = 0; i < 2; i++) {
            assertEquals(cardinalityInBuckets[i].length, 2);
            for (long cardinalityInBucket : cardinalityInBuckets[i]) {
                assertEquals(cardinalityInBucket, 65_536);
            }
        }
    }

    @Test
    public void testCardinalityInBuckets_sparse_contained() throws Exception {
        RoaringBitmap bitmap = new RoaringBitmap();
        for (int i = 0; i < 131_073; i += 128) {
            bitmap.add(i);
        }
        int[][] indexes = new int[][] {
            { 0, 65_536, 131_072 },
            { 1, 65_537, 131_073 } };
        long[][] cardinalityInBuckets = new long[2][indexes[0].length - 1];
        RoaringInspection.cardinalityInBuckets(bitmap, indexes, cardinalityInBuckets);
        for (int i = 0; i < 2; i++) {
            assertEquals(cardinalityInBuckets[i].length, 2);
            for (long cardinalityInBucket : cardinalityInBuckets[i]) {
                assertEquals(cardinalityInBucket, 512);
            }
        }
    }

    @Test
    public void testCardinalityInBuckets_advance_outer() throws Exception {
        RoaringBitmap bitmap = new RoaringBitmap();
        for (int i = 0; i < 100_001; i++) {
            bitmap.add(i);
        }
        int[][] indexes = new int[][] {
            { 40_000, 50_000, 60_000 },
            { 40_000, 50_000, 60_000 } };
        long[][] cardinalityInBuckets = new long[2][indexes[0].length - 1];
        RoaringInspection.cardinalityInBuckets(bitmap, indexes, cardinalityInBuckets);
        for (int i = 0; i < 2; i++) {
            assertEquals(cardinalityInBuckets[i].length, 2);
            for (long cardinalityInBucket : cardinalityInBuckets[i]) {
                assertEquals(cardinalityInBucket, 10_000);
            }
        }
    }

    @Test
    public void testCardinalityInBuckets_advance_inner() throws Exception {
        RoaringBitmap bitmap = new RoaringBitmap();
        for (int i = 90_000; i < 100_000; i++) {
            bitmap.add(i);
        }
        bitmap.add(150_000);
        for (int i = 210_000; i < 220_000; i++) {
            bitmap.add(i);
        }
        int[][] indexes = new int[][] {
            { 0, 80_000, 110_000, 200_000, 230_000, 300_000 },
            { 1, 80_001, 110_001, 200_001, 230_001, 300_001 } };
        long[][] cardinalityInBuckets = new long[2][indexes[0].length - 1];
        RoaringInspection.cardinalityInBuckets(bitmap, indexes, cardinalityInBuckets);
        assertEquals(cardinalityInBuckets[0].length, 5);
        assertEquals(cardinalityInBuckets[1].length, 5);
        for (int i = 0; i < 2; i++) {
            assertEquals(cardinalityInBuckets[i][0], 0);
            assertEquals(cardinalityInBuckets[i][1], 10_000);
            assertEquals(cardinalityInBuckets[i][2], 1);
            assertEquals(cardinalityInBuckets[i][3], 10_000);
            assertEquals(cardinalityInBuckets[i][4], 0);
        }
    }

    @Test
    public void testCardinalityInBuckets_same_buckets() throws Exception {
        RoaringBitmap bitmap = new RoaringBitmap();
        for (int i = 0; i < 10; i++) {
            bitmap.add(i);
        }
        int[][] indexes = new int[][] {
            { 2, 2, 3, 3, 4, 4, 5, 5, 6 },
            { 3, 3, 4, 4, 5, 5, 6, 6, 7 } };
        long[][] cardinalityInBuckets = new long[2][indexes[0].length - 1];
        RoaringInspection.cardinalityInBuckets(bitmap, indexes, cardinalityInBuckets);
        for (int i = 0; i < 2; i++) {
            assertEquals(cardinalityInBuckets[i].length, 8);
            assertEquals(cardinalityInBuckets[i][0], 0);
            assertEquals(cardinalityInBuckets[i][1], 1);
            assertEquals(cardinalityInBuckets[i][2], 0);
            assertEquals(cardinalityInBuckets[i][3], 1);
            assertEquals(cardinalityInBuckets[i][4], 0);
            assertEquals(cardinalityInBuckets[i][5], 1);
            assertEquals(cardinalityInBuckets[i][6], 0);
            assertEquals(cardinalityInBuckets[i][7], 1);
        }
    }

    @Test
    public void testSerDeser() throws Exception {
        Random r = new Random();
        for (int p : new int[] { 1, 10, 100 }) {
            RoaringBitmap bitmap1 = new RoaringBitmap();
            for (int i = 0; i < 1_000_000; i++) {
                if (r.nextInt(p) == 0) {
                    bitmap1.add(i);
                }
            }

            DataOutput[] outContainers = new DataOutputStream[RoaringInspection.containerCount(bitmap1)];
            ByteArrayOutputStream[] bosContainers = new ByteArrayOutputStream[outContainers.length];
            for (int i = 0; i < outContainers.length; i++) {
                bosContainers[i] = new ByteArrayOutputStream();
                outContainers[i] = new DataOutputStream(bosContainers[i]);
            }

            bitmap1.runOptimize();
            int lsb1 = bitmap1.last();
            long cardinality1 = bitmap1.getCardinality();
            short[] keys = RoaringInspection.serialize(bitmap1, outContainers);

            System.out.println("----- " + p + " -----");
            for (int i = 0; i < bosContainers.length; i++) {
                System.out.println(bosContainers[i].size());
            }
            System.out.println();

            DataInput[] inContainers = new DataInputStream[bosContainers.length];
            ByteArrayInputStream[] binContainers = new ByteArrayInputStream[inContainers.length];
            for (int i = 0; i < inContainers.length; i++) {
                binContainers[i] = new ByteArrayInputStream(bosContainers[i].toByteArray());
                inContainers[i] = new DataInputStream(binContainers[i]);
            }

            BitmapAndLastId<RoaringBitmap> container = new BitmapAndLastId<>();
            RoaringInspection.udeserialize(container, atomStream -> {
                for (int i = 0; i < keys.length; i++) {
                    atomStream.stream(RoaringInspection.shortToIntKey(keys[i]), inContainers[i]);
                }
                return true;
            });
            RoaringBitmap bitmap2 = container.getBitmap();
            int lsb2 = container.getLastId();
            long cardinality2 = bitmap2.getCardinality();

            Assert.assertEquals(bitmap1, bitmap2);
            Assert.assertEquals(lsb1, lsb2);
            Assert.assertEquals(cardinality1, cardinality2);
        }
    }

    @Test
    public void testUSerDeser() throws Exception {
        Random r = new Random();
        for (int p : new int[] { 1, 10, 100 }) {
            RoaringBitmap bitmap1 = new RoaringBitmap();
            for (int i = 0; i < 1_000_000; i++) {
                if (r.nextInt(p) == 0) {
                    bitmap1.add(i);
                }
            }

            DataOutput[] outContainers = new DataOutputStream[RoaringInspection.containerCount(bitmap1)];
            ByteArrayOutputStream[] bosContainers = new ByteArrayOutputStream[outContainers.length];
            for (int i = 0; i < outContainers.length; i++) {
                bosContainers[i] = new ByteArrayOutputStream();
                outContainers[i] = new DataOutputStream(bosContainers[i]);
            }

            bitmap1.runOptimize();
            int lsb1 = bitmap1.last();
            long cardinality1 = bitmap1.getCardinality();
            int[] ukeys = RoaringInspection.userialize(bitmap1, outContainers);

            System.out.println("----- " + p + " -----");
            for (int i = 0; i < bosContainers.length; i++) {
                System.out.println(bosContainers[i].size());
            }
            System.out.println();

            DataInput[] inContainers = new DataInputStream[bosContainers.length];
            ByteArrayInputStream[] binContainers = new ByteArrayInputStream[inContainers.length];
            for (int i = 0; i < inContainers.length; i++) {
                binContainers[i] = new ByteArrayInputStream(bosContainers[i].toByteArray());
                inContainers[i] = new DataInputStream(binContainers[i]);
            }

            BitmapAndLastId<RoaringBitmap> container = new BitmapAndLastId<>();
            RoaringInspection.udeserialize(container, atomStream -> {
                for (int i = 0; i < ukeys.length; i++) {
                    atomStream.stream(ukeys[i], inContainers[i]);
                }
                return true;
            });
            RoaringBitmap bitmap2 = container.getBitmap();
            int lsb2 = container.getLastId();
            long cardinality2 = bitmap2.getCardinality();

            Assert.assertEquals(bitmap1, bitmap2);
            Assert.assertEquals(lsb1, lsb2);
            Assert.assertEquals(cardinality1, cardinality2);
        }
    }

    @Test
    public void testLastSetBit() throws Exception {
        RoaringBitmap bitmap = new RoaringBitmap();
        for (int i = 0; i < 100; i++) {
            bitmap.add(i * 65_536 + i);
        }

        DataOutput[] outContainers = new DataOutputStream[RoaringInspection.containerCount(bitmap)];
        ByteArrayOutputStream[] bosContainers = new ByteArrayOutputStream[outContainers.length];
        for (int i = 0; i < outContainers.length; i++) {
            bosContainers[i] = new ByteArrayOutputStream();
            outContainers[i] = new DataOutputStream(bosContainers[i]);
        }

        int[] ukeys = RoaringInspection.userialize(bitmap, outContainers);

        DataInput[] inContainers = new DataInputStream[bosContainers.length];
        ByteArrayInputStream[] binContainers = new ByteArrayInputStream[inContainers.length];
        for (int i = 0; i < inContainers.length; i++) {
            binContainers[i] = new ByteArrayInputStream(bosContainers[i].toByteArray());
            inContainers[i] = new DataInputStream(binContainers[i]);
        }

        for (int i = 0; i < ukeys.length; i++) {
            int lastSetBit = RoaringInspection.lastSetBit(ukeys[i], inContainers[i]);
            Assert.assertEquals(lastSetBit, 65_536 * i + i);
        }
    }

    @Test
    public void testIntToShortKeys() throws Exception {
        int[] ukeys = new int[65_536];
        for (int i = 0; i < ukeys.length; i++) {
            ukeys[i] = i;
        }
        short[] keys = RoaringInspection.intToShortKeys(ukeys);
        int[] got = RoaringInspection.shortToIntKeys(keys);
        Assert.assertEquals(got, ukeys);
    }

    @Test(enabled = false, description = "Performance comparison")
    public void testFirstIntersectingBit() throws Exception {
        Random r = new Random();
        for (int numRuns : new int[] { 10_000, 100_000 }) {
            System.out.println("---- " + numRuns + " ----");
            for (double d1 : new double[] { 0.0001, 0.01, 0.1, 0.5, 0.9, 0.999 }) {
                for (double d2 : new double[] { 0.0001, 0.01, 0.1, 0.5, 0.9, 0.999 }) {
                    System.out.println("d1:" + d1 + " d2:" + d2);
                    RoaringBitmap b1 = new RoaringBitmap();
                    RoaringBitmap b2 = new RoaringBitmap();
                    for (int i = 0; i < 3_000_000; i++) {
                        if (r.nextDouble() < d1) {
                            b1.add(i);
                        }
                        if (r.nextDouble() < d2) {
                            b2.add(i);
                        }
                    }

                    RoaringBitmap and = RoaringBitmap.and(b1, b2);
                    int ax = and.isEmpty() ? -1 : and.first();

                    int nfx = -1;
                    long start = System.currentTimeMillis();
                    for (int i = 0; i < numRuns; i++) {
                        nfx = RoaringInspection.naiveFirstIntersectingBit(b1, b2);
                    }
                    long ne = System.currentTimeMillis() - start;

                    int fx = -1;
                    start = System.currentTimeMillis();
                    for (int i = 0; i < numRuns; i++) {
                        fx = RoaringInspection.firstIntersectingBit(b1, b2);
                    }
                    long e = System.currentTimeMillis() - start;

                    Assert.assertEquals(ax, nfx);
                    Assert.assertEquals(ax, fx);
                    System.out.println(" -> " + nfx + " = " + ne);
                    System.out.println(" -> " + fx + " = " + e);
                }
            }
        }
    }

    @Test
    public void testFirstIntersectingBit_RunRun() throws Exception {
        Container c1 = new RunContainer();
        c1.iadd(10, 20);
        c1.iadd(30, 40);
        c1.iadd(40, 50);
        c1.iadd(50, 60);

        Container c2 = new RunContainer();
        c2.iadd(35, 45);
        c2.iadd(55, 65);

        int x = RoaringInspection.firstIntersectingBit(c1, c2);
        assertEquals(x, 35);

        Container c3 = new RunContainer();
        c3.iadd(65, 75);
        c3.iadd(85, 95);

        x = RoaringInspection.firstIntersectingBit(c1, c3);
        assertEquals(x, -1);
    }

    @Test
    public void testFirstIntersectingBit_RunArray() throws Exception {
        Container c1 = new RunContainer();
        c1.iadd(10, 20);
        c1.iadd(30, 40);
        c1.iadd(40, 50);
        c1.iadd(50, 60);

        Container c2 = new ArrayContainer();
        c2.iadd(35, 36);
        c2.iadd(55, 56);

        int x = RoaringInspection.firstIntersectingBit(c1, c2);
        assertEquals(x, 35);

        Container c3 = new ArrayContainer();
        c3.iadd(65, 66);
        c3.iadd(75, 76);

        x = RoaringInspection.firstIntersectingBit(c1, c3);
        assertEquals(x, -1);
    }

    @Test
    public void testFirstIntersectingBit_RunBitmap() throws Exception {
        Container c1 = new RunContainer();
        c1.iadd(10, 20);
        c1.iadd(30, 40);
        c1.iadd(40, 50);
        c1.iadd(50, 60);

        Container c2 = new BitmapContainer();
        c2.iadd(35, 36);
        c2.iadd(55, 56);

        int x = RoaringInspection.firstIntersectingBit(c1, c2);
        assertEquals(x, 35);

        Container c3 = new BitmapContainer();
        c3.iadd(65, 66);
        c3.iadd(75, 76);

        x = RoaringInspection.firstIntersectingBit(c1, c3);
        assertEquals(x, -1);
    }

    @Test
    public void testFirstIntersectingBit_ArrayArray() throws Exception {
        Container c1 = new ArrayContainer();
        c1.iadd(15, 16);
        c1.iadd(25, 26);
        c1.iadd(35, 36);
        c1.iadd(45, 46);
        c1.iadd(56, 56);

        Container c2 = new ArrayContainer();
        c2.iadd(30, 31);
        c2.iadd(35, 36);
        c2.iadd(40, 41);

        int x = RoaringInspection.firstIntersectingBit(c1, c2);
        assertEquals(x, 35);

        Container c3 = new ArrayContainer();
        c3.iadd(65, 66);
        c3.iadd(75, 76);

        x = RoaringInspection.firstIntersectingBit(c1, c3);
        assertEquals(x, -1);
    }

    @Test
    public void testFirstIntersectingBit_ArrayBitmap() throws Exception {
        Container c1 = new ArrayContainer();
        c1.iadd(15, 16);
        c1.iadd(25, 26);
        c1.iadd(35, 36);
        c1.iadd(45, 46);
        c1.iadd(56, 56);

        Container c2 = new BitmapContainer();
        c2.iadd(30, 31);
        c2.iadd(35, 36);
        c2.iadd(40, 41);

        int x = RoaringInspection.firstIntersectingBit(c1, c2);
        assertEquals(x, 35);

        Container c3 = new BitmapContainer();
        c3.iadd(65, 66);
        c3.iadd(75, 76);

        x = RoaringInspection.firstIntersectingBit(c1, c3);
        assertEquals(x, -1);
    }

    @Test
    public void testFirstIntersectingBit_BitmapBitmap() throws Exception {
        Container c1 = new BitmapContainer();
        c1.iadd(15, 16);
        c1.iadd(25, 26);
        c1.iadd(35, 36);
        c1.iadd(45, 46);
        c1.iadd(56, 56);

        Container c2 = new BitmapContainer();
        c2.iadd(30, 31);
        c2.iadd(35, 36);
        c2.iadd(40, 41);

        int x = RoaringInspection.firstIntersectingBit(c1, c2);
        assertEquals(x, 35);

        Container c3 = new BitmapContainer();
        c3.iadd(65, 66);
        c3.iadd(75, 76);

        x = RoaringInspection.firstIntersectingBit(c1, c3);
        assertEquals(x, -1);
    }

    @Test
    public void testKeysNotEqual() throws Exception {
        RoaringBitmap b1 = RoaringBitmap.bitmapOf(0 * 65_536, 1 * 65_536);
        RoaringBitmap b2 = RoaringBitmap.bitmapOf(2 * 65_536, 3 * 65_536);

        int[] keys = RoaringInspection.keysNotEqual(b1, b2);
        assertEquals(keys.length, 4);
        assertEquals(keys[0], 0);
        assertEquals(keys[1], 1);
        assertEquals(keys[2], 2);
        assertEquals(keys[3], 3);

        b1 = RoaringBitmap.bitmapOf(2 * 65_536, 3 * 65_536);
        b2 = RoaringBitmap.bitmapOf(0 * 65_536, 1 * 65_536);

        keys = RoaringInspection.keysNotEqual(b1, b2);
        assertEquals(keys.length, 4);
        assertEquals(keys[0], 0);
        assertEquals(keys[1], 1);
        assertEquals(keys[2], 2);
        assertEquals(keys[3], 3);

        b1 = RoaringBitmap.bitmapOf(0 * 65_536, 1 * 65_536);
        b2 = RoaringBitmap.bitmapOf(1 * 65_536, 2 * 65_536);

        keys = RoaringInspection.keysNotEqual(b1, b2);
        assertEquals(keys.length, 2);
        assertEquals(keys[0], 0);
        assertEquals(keys[1], 2);

        b1 = RoaringBitmap.bitmapOf(0 * 65_536, 1 * 65_536);
        b2 = RoaringBitmap.bitmapOf(4 * 65_536, 5 * 65_536);

        keys = RoaringInspection.keysNotEqual(b1, b2);
        assertEquals(keys.length, 4);
        assertEquals(keys[0], 0);
        assertEquals(keys[1], 1);
        assertEquals(keys[2], 4);
        assertEquals(keys[3], 5);
    }

}