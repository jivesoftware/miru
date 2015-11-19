package org.roaringbitmap.buffer;

import com.jivesoftware.os.miru.plugin.bitmap.CardinalityAndLastSetBit;
import java.nio.LongBuffer;

/**
 *
 */
public class RoaringBufferInspection {

    public static CardinalityAndLastSetBit cardinalityAndLastSetBit(ImmutableRoaringBitmap bitmap) {
        int pos = bitmap.highLowContainer.size() - 1;
        int lastSetBit = -1;
        while (pos >= 0) {
            MappeableContainer lastContainer = bitmap.highLowContainer.getContainerAtIndex(pos);
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

    private static int lastSetBit(ImmutableRoaringBitmap bitmap, MappeableContainer container, int pos) {
        if (container instanceof MappeableArrayContainer) {
            MappeableArrayContainer arrayContainer = (MappeableArrayContainer) container;
            int cardinality = arrayContainer.cardinality;
            if (cardinality > 0) {
                int hs = BufferUtil.toIntUnsigned(bitmap.highLowContainer.getKeyAtIndex(pos)) << 16;
                short last = arrayContainer.content.get(cardinality - 1);
                return BufferUtil.toIntUnsigned(last) | hs;
            }
        } else if (container instanceof MappeableRunContainer) {
            MappeableRunContainer runContainer = (MappeableRunContainer) container;
            if (runContainer.nbrruns > 0) {
                int hs = BufferUtil.toIntUnsigned(bitmap.highLowContainer.getKeyAtIndex(pos)) << 16;
                int maxlength = BufferUtil.toIntUnsigned(runContainer.getLength(runContainer.nbrruns - 1));
                int base = BufferUtil.toIntUnsigned(runContainer.getValue(runContainer.nbrruns - 1));
                return (base + maxlength) | hs;
            }
        } else {
            // <-- trailing              leading -->
            // [ 0, 0, 0, 0, 0 ... , 0, 0, 0, 0, 0 ]
            MappeableBitmapContainer bitmapContainer = (MappeableBitmapContainer) container;
            LongBuffer longs = bitmapContainer.bitmap;
            for (int i = longs.limit() - 1; i >= 0; i--) {
                long l = longs.get(i);
                int leadingZeros = Long.numberOfLeadingZeros(l);
                if (leadingZeros < 64) {
                    int hs = BufferUtil.toIntUnsigned(bitmap.highLowContainer.getKeyAtIndex(pos)) << 16;
                    short last = (short) ((i * 64) + 64 - leadingZeros - 1);
                    return BufferUtil.toIntUnsigned(last) | hs;
                }
            }
        }
        return -1;
    }

    public static long sizeInBits(ImmutableRoaringBitmap bitmap) {
        int pos = bitmap.highLowContainer.size() - 1;
        if (pos >= 0) {
            return (BufferUtil.toIntUnsigned(bitmap.highLowContainer.getKeyAtIndex(pos)) + 1) << 16;
        } else {
            return 0;
        }
    }

    public static void cardinalityInBuckets(ImmutableRoaringBitmap bitmap, int[] indexes, long[] buckets) {
        // indexes = { 10, 20, 30, 40, 50 } length=5
        // buckets = { 10-19, 20-29, 30-39, 40-49 } length=4
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
                MappeableContainer container = bitmap.highLowContainer.getContainerAtIndex(pos);
                int max = min + (1 << 16);
                boolean bucketContainsPos = (currentBucketStart <= min && max <= currentBucketEnd);
                if (bucketContainsPos) {
                    //System.out.println("BucketContainsPos");
                    buckets[currentBucket] += container.getCardinality();
                } else if (container instanceof MappeableArrayContainer) {
                    //System.out.println("ArrayContainer");
                    MappeableArrayContainer arrayContainer = (MappeableArrayContainer) container;
                    for (int i = 0; i < arrayContainer.cardinality; i++) {
                        int index = BufferUtil.toIntUnsigned(arrayContainer.content.get(i)) | min;
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
                } else if (container instanceof MappeableRunContainer) {
                    MappeableRunContainer runContainer = (MappeableRunContainer) container;
                    for (int i = 0; i < runContainer.nbrruns; i++) {
                        int maxlength = BufferUtil.toIntUnsigned(runContainer.getLength(i));
                        int base = BufferUtil.toIntUnsigned(runContainer.getValue(i));
                        int index = (maxlength + base) | min;
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
                } else {
                    //System.out.println("BitmapContainer");
                    MappeableBitmapContainer bitmapContainer = (MappeableBitmapContainer) container;
                    // nextSetBit no longer performs a bounds check
                    int maxIndex = bitmapContainer.bitmap.limit() << 6;
                    for (int i = bitmapContainer.nextSetBit(0);
                        i >= 0;
                        i = (i + 1 >= maxIndex) ? -1 : bitmapContainer.nextSetBit(i + 1)) {
                        int index = BufferUtil.toIntUnsigned((short) i) | min;
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

    private static int containerMin(ImmutableRoaringBitmap bitmap, int pos) {
        return BufferUtil.toIntUnsigned(bitmap.highLowContainer.getKeyAtIndex(pos)) << 16;
    }

    private RoaringBufferInspection() {
    }
}
