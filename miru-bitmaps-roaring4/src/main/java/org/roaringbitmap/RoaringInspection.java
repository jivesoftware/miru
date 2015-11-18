package org.roaringbitmap;

import com.jivesoftware.os.miru.plugin.bitmap.CardinalityAndLastSetBit;

/**
 *
 */
public class RoaringInspection {

    public static CardinalityAndLastSetBit cardinalityAndLastSetBit(RoaringBitmap bitmap) {
        int pos = bitmap.highLowContainer.size() - 1;
        int lastSetBit = -1;
        while (pos >= 0) {
            Container lastContainer = bitmap.highLowContainer.array[pos].value;
            lastSetBit = lastSetBit(bitmap, lastContainer, pos);
            if (lastSetBit >= 0) {
                break;
            }
            pos--;
        }
        int cardinality = bitmap.getCardinality();
        assert cardinality == 0 || lastSetBit >= 0;
        return new CardinalityAndLastSetBit(cardinality, lastSetBit);
    }

    private static int lastSetBit(RoaringBitmap bitmap, Container container, int pos) {
        if (container instanceof ArrayContainer) {
            ArrayContainer arrayContainer = (ArrayContainer) container;
            int cardinality = arrayContainer.cardinality;
            if (cardinality > 0) {
                int hs = Util.toIntUnsigned(bitmap.highLowContainer.getKeyAtIndex(pos)) << 16;
                short last = arrayContainer.content[cardinality - 1];
                return Util.toIntUnsigned(last) | hs;
            }
        } else {
            // <-- trailing              leading -->
            // [ 0, 0, 0, 0, 0 ... , 0, 0, 0, 0, 0 ]
            BitmapContainer bitmapContainer = (BitmapContainer) container;
            long[] longs = bitmapContainer.bitmap;
            for (int i = longs.length - 1; i >= 0; i--) {
                long l = longs[i];
                int leadingZeros = Long.numberOfLeadingZeros(l);
                if (leadingZeros < 64) {
                    int hs = Util.toIntUnsigned(bitmap.highLowContainer.getKeyAtIndex(pos)) << 16;
                    short last = (short) ((i * 64) + 64 - leadingZeros - 1);
                    return Util.toIntUnsigned(last) | hs;
                }
            }
        }
        return -1;
    }

    public static long sizeInBits(RoaringBitmap bitmap) {
        int pos = bitmap.highLowContainer.size() - 1;
        if (pos >= 0) {
            return (Util.toIntUnsigned(bitmap.highLowContainer.getKeyAtIndex(pos)) + 1) << 16;
        } else {
            return 0;
        }
    }

    public static long[] cardinalityInBuckets(RoaringBitmap bitmap, int[] indexes) {
        // indexes = { 10, 20, 30, 40, 50 } length=5
        // buckets = { 10-19, 20-29, 30-39, 40-49 } length=4
        long[] buckets = new long[indexes.length - 1];
        int numContainers = bitmap.highLowContainer.size();
        //System.out.println("NumContainers=" + numContainers);
        int currentBucket = 0;
        int currentBucketStart = indexes[currentBucket];
        int currentBucketEnd = indexes[currentBucket + 1];
        done:
        for (int pos = 0; pos < numContainers; pos++) {
            //System.out.println("pos=" + pos);
            int min = containerMin(bitmap, pos);
            while (min >= currentBucketEnd) {
                //System.out.println("Advance1 min:" + min + " >= currentBucketEnd:" + currentBucketEnd);
                currentBucket++;
                if (currentBucket == buckets.length) {
                    break done;
                }
                currentBucketStart = indexes[currentBucket];
                currentBucketEnd = indexes[currentBucket + 1];
            }

            if (min < currentBucketEnd) {
                Container container = bitmap.highLowContainer.array[pos].value;
                int max = min + (1 << 16);
                boolean bucketContainsPos = (currentBucketStart <= min && max <= currentBucketEnd);
                if (bucketContainsPos) {
                    //System.out.println("BucketContainsPos");
                    buckets[currentBucket] += container.getCardinality();
                } else {
                    if (container instanceof ArrayContainer) {
                        //System.out.println("ArrayContainer");
                        ArrayContainer arrayContainer = (ArrayContainer) container;
                        for (int i = 0; i < arrayContainer.cardinality; i++) {
                            int index = Util.toIntUnsigned(arrayContainer.content[i]) | min;
                            while (index >= currentBucketEnd) {
                                //System.out.println("Advance2 index:" + index + " >= currentBucketEnd:" + currentBucketEnd);
                                currentBucket++;
                                if (currentBucket == buckets.length) {
                                    break done;
                                }
                                currentBucketStart = indexes[currentBucket];
                                currentBucketEnd = indexes[currentBucket + 1];
                            }
                            if (index >= currentBucketStart) {
                                buckets[currentBucket]++;
                            }
                        }
                    } else {
                        //System.out.println("BitmapContainer");
                        BitmapContainer bitmapContainer = (BitmapContainer) container;
                        // nextSetBit no longer performs a bounds check
                        int maxIndex = bitmapContainer.bitmap.length << 6;
                        for (int i = bitmapContainer.nextSetBit(0);
                             i >= 0;
                             i = (i + 1 >= maxIndex) ? -1 : bitmapContainer.nextSetBit(i + 1)) {
                            int index = Util.toIntUnsigned((short) i) | min;
                            while (index >= currentBucketEnd) {
                                //System.out.println("Advance3 index:" + index + " >= currentBucketEnd:" + currentBucketEnd);
                                currentBucket++;
                                if (currentBucket == buckets.length) {
                                    break done;
                                }
                                currentBucketStart = indexes[currentBucket];
                                currentBucketEnd = indexes[currentBucket + 1];
                            }
                            if (index >= currentBucketStart) {
                                buckets[currentBucket]++;
                            }
                        }
                    }
                }
            }
        }
        return buckets;
    }

    private static int containerMin(RoaringBitmap bitmap, int pos) {
        return Util.toIntUnsigned(bitmap.highLowContainer.getKeyAtIndex(pos)) << 16;
    }

    private RoaringInspection() {
    }
}
