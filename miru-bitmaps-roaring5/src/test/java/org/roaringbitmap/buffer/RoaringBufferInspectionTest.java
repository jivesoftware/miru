package org.roaringbitmap.buffer;

import org.roaringbitmap.RoaringBitmap;
import org.roaringbitmap.RoaringInspection;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;

public class RoaringBufferInspectionTest {

    @Test
    public void testSizeInBits() throws Exception {
        MutableRoaringBitmap bitmap = new MutableRoaringBitmap();

        assertEquals(RoaringBufferInspection.sizeInBits(bitmap), 0);
        bitmap.add(0);
        assertEquals(RoaringBufferInspection.sizeInBits(bitmap), 1 << 16);
        bitmap.add(1 << 16 - 1);
        assertEquals(RoaringBufferInspection.sizeInBits(bitmap), 1 << 16);
        bitmap.add(1 << 16);
        assertEquals(RoaringBufferInspection.sizeInBits(bitmap), 2 << 16);
        bitmap.add(2 << 16 - 1);
        assertEquals(RoaringBufferInspection.sizeInBits(bitmap), 2 << 16);
        bitmap.add(2 << 16);
        assertEquals(RoaringBufferInspection.sizeInBits(bitmap), 3 << 16);
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
}
