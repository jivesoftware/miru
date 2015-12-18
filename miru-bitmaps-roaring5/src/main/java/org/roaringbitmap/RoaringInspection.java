package org.roaringbitmap;

import com.jivesoftware.os.miru.plugin.bitmap.CardinalityAndLastSetBit;
import java.util.Arrays;

/**
 *
 */
public class RoaringInspection {

    public static RoaringBitmap[] split(RoaringBitmap bitmap) {
        RoaringArray array = bitmap.highLowContainer;
        int size = array.size();
        RoaringBitmap[] split = new RoaringBitmap[size];
        for (int i = 0; i < size; i++) {
            split[i] = new RoaringBitmap();
            split[i].highLowContainer.append(array.getKeyAtIndex(i), array.getContainerAtIndex(i));
        }
        return split;
    }

    public static RoaringBitmap join(RoaringBitmap[] split) {
        RoaringBitmap bitmap = new RoaringBitmap();
        RoaringArray array = bitmap.highLowContainer;
        array.extendArray(split.length);
        for (int i = 0; i < split.length; i++) {
            array.append(split[i].highLowContainer.getKeyAtIndex(0), split[i].highLowContainer.getContainerAtIndex(0));
        }
        return bitmap;
    }

    /*public static void main(String[] args) {
        Random r = new Random();
        for (int i = 1; i <= 1_000; i++) {
            RoaringBitmap bitmap = new RoaringBitmap();
            for (int j = 0; j < 1_000_000; j++) {
                if (r.nextInt(i) < 10) {
                    bitmap.add(j);
                }
            }

            RoaringBitmap[] split = split(bitmap);
            RoaringBitmap joined = join(split);

            System.out.println("test: " + bitmap.getCardinality() + " = " + joined.getCardinality() + " -> " + bitmap.equals(joined));
        }
    }*/

    public static CardinalityAndLastSetBit<RoaringBitmap> cardinalityAndLastSetBit(RoaringBitmap bitmap) {
        int pos = bitmap.highLowContainer.size() - 1;
        int lastSetBit = -1;
        while (pos >= 0) {
            Container lastContainer = bitmap.highLowContainer.values[pos];
            lastSetBit = lastSetBit(bitmap, lastContainer, pos);
            if (lastSetBit >= 0) {
                break;
            }
            pos--;
        }
        int cardinality = bitmap.getCardinality();
        assert cardinality == 0 || lastSetBit >= 0;
        return new CardinalityAndLastSetBit<>(bitmap, cardinality, lastSetBit);
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
        } else if (container instanceof RunContainer) {
            RunContainer runContainer = (RunContainer) container;
            if (runContainer.nbrruns > 0) {
                int hs = Util.toIntUnsigned(bitmap.highLowContainer.getKeyAtIndex(pos)) << 16;
                int maxlength = Util.toIntUnsigned(runContainer.getLength(runContainer.nbrruns - 1));
                int base = Util.toIntUnsigned(runContainer.getValue(runContainer.nbrruns - 1));
                return (base + maxlength) | hs;
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

    public static void cardinalityInBuckets(RoaringBitmap bitmap, int[][] indexes, long[][] buckets) {
        // indexes = { 10, 20, 30, 40, 50 } length=5
        // buckets = { 10-19, 20-29, 30-39, 40-49 } length=4
        int numContainers = bitmap.highLowContainer.size();
        //System.out.println("NumContainers=" + numContainers);
        int bucketLength = buckets.length;
        int[] currentBucket = new int[bucketLength];
        Arrays.fill(currentBucket, 0);
        int[] currentBucketStart = new int[bucketLength];
        int[] currentBucketEnd = new int[bucketLength];
        for (int bi = 0; bi < bucketLength; bi++) {
            currentBucketStart[bi] = indexes[bi][currentBucket[bi]];
            currentBucketEnd[bi] = indexes[bi][currentBucket[bi] + 1];
        }

        int numExhausted = 0;
        boolean[] exhausted = new boolean[bucketLength];

        for (int pos = 0; pos < numContainers; pos++) {
            //System.out.println("pos=" + pos);
            int min = containerMin(bitmap, pos);
            for (int bi = 0; bi < bucketLength; bi++) {
                while (!exhausted[bi] && min >= currentBucketEnd[bi]) {
                    //System.out.println("Advance1 min:" + min + " >= currentBucketEnd:" + currentBucketEnd);
                    currentBucket[bi]++;
                    if (currentBucket[bi] == buckets[bi].length) {
                        numExhausted++;
                        exhausted[bi] = true;
                        break;
                    }
                    currentBucketStart[bi] = indexes[bi][currentBucket[bi]];
                    currentBucketEnd[bi] = indexes[bi][currentBucket[bi] + 1];
                }
            }
            if (numExhausted == bucketLength) {
                break;
            }

            boolean[] candidate = new boolean[bucketLength];
            boolean anyCandidates = false;
            for (int bi = 0; bi < bucketLength; bi++) {
                candidate[bi] = (min < currentBucketEnd[bi]);
                anyCandidates |= candidate[bi];
            }

            if (anyCandidates) {
                Container container = bitmap.highLowContainer.values[pos];
                int max = min + (1 << 16);
                boolean[] bucketContainsPos = new boolean[bucketLength];
                boolean allContainPos = true;
                boolean anyContainPos = false;
                for (int bi = 0; bi < bucketLength; bi++) {
                    bucketContainsPos[bi] = (currentBucketStart[bi] <= min && max <= currentBucketEnd[bi]);
                    allContainPos &= bucketContainsPos[bi];
                    anyContainPos |= bucketContainsPos[bi];
                }

                if (anyContainPos) {
                    int cardinality = container.getCardinality();
                    for (int bi = 0; bi < bucketLength; bi++) {
                        if (bucketContainsPos[bi]) {
                            //System.out.println("BucketContainsPos");
                            buckets[bi][currentBucket[bi]] += cardinality;
                        }
                    }
                }

                if (!allContainPos) {
                    if (container instanceof ArrayContainer) {
                        //System.out.println("ArrayContainer");
                        ArrayContainer arrayContainer = (ArrayContainer) container;
                        for (int i = 0; i < arrayContainer.cardinality && numExhausted < bucketLength; i++) {
                            int index = Util.toIntUnsigned(arrayContainer.content[i]) | min;
                            next:
                            for (int bi = 0; bi < bucketLength; bi++) {
                                if (!candidate[bi] || bucketContainsPos[bi] || exhausted[bi]) {
                                    continue;
                                }
                                while (index >= currentBucketEnd[bi]) {
                                    //System.out.println("Advance2 index:" + index + " >= currentBucketEnd:" + currentBucketEnd);
                                    currentBucket[bi]++;
                                    if (currentBucket[bi] == buckets[bi].length) {
                                        numExhausted++;
                                        exhausted[bi] = true;
                                        continue next;
                                    }
                                    currentBucketStart[bi] = indexes[bi][currentBucket[bi]];
                                    currentBucketEnd[bi] = indexes[bi][currentBucket[bi] + 1];
                                }
                                if (index >= currentBucketStart[bi]) {
                                    buckets[bi][currentBucket[bi]]++;
                                }
                            }

                        }
                    } else if (container instanceof RunContainer) {
                        RunContainer runContainer = (RunContainer) container;
                        for (int i = 0; i < runContainer.nbrruns && numExhausted < bucketLength; i++) {
                            int maxlength = Util.toIntUnsigned(runContainer.getLength(i));
                            int base = Util.toIntUnsigned(runContainer.getValue(i));
                            int index = (maxlength + base) | min;
                            next:
                            for (int bi = 0; bi < bucketLength; bi++) {
                                if (!candidate[bi] || bucketContainsPos[bi] || exhausted[bi]) {
                                    continue;
                                }
                                while (index >= currentBucketEnd[bi]) {
                                    //System.out.println("Advance3 index:" + index + " >= currentBucketEnd:" + currentBucketEnd);
                                    currentBucket[bi]++;
                                    if (currentBucket[bi] == buckets[bi].length) {
                                        numExhausted++;
                                        exhausted[bi] = true;
                                        continue next;
                                    }
                                    currentBucketStart[bi] = indexes[bi][currentBucket[bi]];
                                    currentBucketEnd[bi] = indexes[bi][currentBucket[bi] + 1];
                                }
                                if (index >= currentBucketStart[bi]) {
                                    buckets[bi][currentBucket[bi]]++;
                                }
                            }
                        }
                    } else {
                        //System.out.println("BitmapContainer");
                        BitmapContainer bitmapContainer = (BitmapContainer) container;
                        // nextSetBit no longer performs a bounds check
                        int maxIndex = bitmapContainer.bitmap.length << 6;
                        for (int i = bitmapContainer.nextSetBit(0);
                             i >= 0 && numExhausted < bucketLength;
                             i = (i + 1 >= maxIndex) ? -1 : bitmapContainer.nextSetBit(i + 1)) {
                            int index = Util.toIntUnsigned((short) i) | min;
                            next:
                            for (int bi = 0; bi < bucketLength; bi++) {
                                if (!candidate[bi] || bucketContainsPos[bi] || exhausted[bi]) {
                                    continue;
                                }
                                while (index >= currentBucketEnd[bi]) {
                                    //System.out.println("Advance3 index:" + index + " >= currentBucketEnd:" + currentBucketEnd);
                                    currentBucket[bi]++;
                                    if (currentBucket[bi] == buckets[bi].length) {
                                        numExhausted++;
                                        exhausted[bi] = true;
                                        continue next;
                                    }
                                    currentBucketStart[bi] = indexes[bi][currentBucket[bi]];
                                    currentBucketEnd[bi] = indexes[bi][currentBucket[bi] + 1];
                                }
                                if (index >= currentBucketStart[bi]) {
                                    buckets[bi][currentBucket[bi]]++;
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    private static int containerMin(RoaringBitmap bitmap, int pos) {
        return Util.toIntUnsigned(bitmap.highLowContainer.getKeyAtIndex(pos)) << 16;
    }

    private RoaringInspection() {
    }
}
