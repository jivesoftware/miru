package org.roaringbitmap;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import com.google.common.primitives.Shorts;
import com.jivesoftware.os.miru.plugin.bitmap.MiruBitmaps.StreamAtoms;
import com.jivesoftware.os.miru.plugin.index.BitmapAndLastId;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.roaringbitmap.Util.toIntUnsigned;

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

    public static RoaringBitmap[] extract(RoaringBitmap bitmap, int[] ukeys) {
        RoaringArray array = bitmap.highLowContainer;
        short[] keys = intToShortKeys(ukeys);
        RoaringBitmap[] extract = new RoaringBitmap[keys.length];
        for (int i = 0; i < keys.length; i++) {
            Container container = array.getContainer(keys[i]);
            if (container != null) {
                extract[i] = new RoaringBitmap();
                extract[i].highLowContainer.append(keys[i], container);
            }
        }
        return extract;
    }

    private static int lastSetIndex(Container container) {
        if (container instanceof ArrayContainer) {
            ArrayContainer arrayContainer = (ArrayContainer) container;
            int cardinality = arrayContainer.cardinality;
            if (cardinality > 0) {
                short last = arrayContainer.content[cardinality - 1];
                return toIntUnsigned(last);
            }
        } else if (container instanceof RunContainer) {
            RunContainer runContainer = (RunContainer) container;
            if (runContainer.nbrruns > 0) {
                int maxlength = toIntUnsigned(runContainer.getLength(runContainer.nbrruns - 1));
                int base = toIntUnsigned(runContainer.getValue(runContainer.nbrruns - 1));
                return (base + maxlength);
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
                    short last = (short) ((i * 64) + 64 - leadingZeros - 1);
                    return toIntUnsigned(last);
                }
            }
        }
        return -1;
    }

    public static long sizeInBits(RoaringBitmap bitmap) {
        int pos = bitmap.highLowContainer.size() - 1;
        if (pos >= 0) {
            return (toIntUnsigned(bitmap.highLowContainer.getKeyAtIndex(pos)) + 1) << 16;
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
                            int index = toIntUnsigned(arrayContainer.content[i]) | min;
                            next:
                            for (int bi = 0; bi < bucketLength; bi++) {
                                if (!candidate[bi] || bucketContainsPos[bi] || exhausted[bi]) {
                                    continue;
                                }
                                while (index >= currentBucketEnd[bi]) {
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
                            int base = toIntUnsigned(runContainer.getValue(i));

                            int startInclusive = base | min;
                            int endExclusive = startInclusive + 1 + toIntUnsigned(runContainer.getLength(i));
                            for (int index = startInclusive; index < endExclusive && numExhausted < bucketLength; index++) {
                                next:
                                for (int bi = 0; bi < bucketLength; bi++) {
                                    if (!candidate[bi] || bucketContainsPos[bi] || exhausted[bi]) {
                                        continue;
                                    }
                                    while (index >= currentBucketEnd[bi]) {
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
                    } else {
                        //System.out.println("BitmapContainer");
                        BitmapContainer bitmapContainer = (BitmapContainer) container;
                        // nextSetBit no longer performs a bounds check
                        int maxIndex = bitmapContainer.bitmap.length << 6;
                        for (int i = bitmapContainer.nextSetBit(0);
                             i >= 0 && numExhausted < bucketLength;
                             i = (i + 1 >= maxIndex) ? -1 : bitmapContainer.nextSetBit(i + 1)) {
                            int index = toIntUnsigned((short) i) | min;
                            next:
                            for (int bi = 0; bi < bucketLength; bi++) {
                                if (!candidate[bi] || bucketContainsPos[bi] || exhausted[bi]) {
                                    continue;
                                }
                                while (index >= currentBucketEnd[bi]) {
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
        return toIntUnsigned(bitmap.highLowContainer.getKeyAtIndex(pos)) << 16;
    }

    public static int key(int position) {
        return Util.highbits(position);
    }

    public static int[] keys(RoaringBitmap bitmap) {
        short[] bitmapKeys = bitmap.highLowContainer.keys;
        int[] copyKeys = new int[bitmap.highLowContainer.size];
        for (int i = 0; i < copyKeys.length; i++) {
            copyKeys[i] = toIntUnsigned(bitmapKeys[i]);
        }
        return copyKeys;
    }

    public static int[] keysNotEqual(RoaringBitmap x1, RoaringBitmap x2) {
        int length1 = x1.highLowContainer.size();
        int length2 = x2.highLowContainer.size();
        int pos1 = 0;
        int pos2 = 0;

        int lastKey1 = length1 == 0 ? -1 : Util.toIntUnsigned(x1.highLowContainer.getKeyAtIndex(length1 - 1));
        int lastKey2 = length2 == 0 ? -1 : Util.toIntUnsigned(x2.highLowContainer.getKeyAtIndex(length2 - 1));
        int[] keys = new int[Math.max(lastKey1, lastKey2) + 1];

        int ki = 0;
        while (pos1 < length1 && pos2 < length2) {
            short s1 = x1.highLowContainer.getKeyAtIndex(pos1);
            short s2 = x2.highLowContainer.getKeyAtIndex(pos2);
            if (s1 == s2) {
                Container c1 = x1.highLowContainer.getContainerAtIndex(pos1);
                Container c2 = x2.highLowContainer.getContainerAtIndex(pos2);

                if (!c1.equals(c2)) {
                    keys[ki++] = Util.toIntUnsigned(s1);
                }

                ++pos1;
                ++pos2;
            } else if (Util.compareUnsigned(s1, s2) < 0) {
                keys[ki++] = Util.toIntUnsigned(s1);
                ++pos1;
            } else {
                keys[ki++] = Util.toIntUnsigned(s2);
                ++pos2;
            }
        }
        while (pos1 < length1) {
            short s1 = x1.highLowContainer.getKeyAtIndex(pos1);
            keys[ki++] = Util.toIntUnsigned(s1);
            ++pos1;
        }
        while (pos2 < length2) {
            short s2 = x2.highLowContainer.getKeyAtIndex(pos2);
            keys[ki++] = Util.toIntUnsigned(s2);
            ++pos2;
        }

        if (ki == keys.length) {
            return keys;
        } else {
            int[] compact = new int[ki];
            System.arraycopy(keys, 0, compact, 0, ki);
            return compact;
        }
    }

    public static int[] userialize(RoaringBitmap bitmap, DataOutput[] outContainers) throws IOException {
        return shortToIntKeys(serialize(bitmap, outContainers));
    }

    public static boolean[] userializeAtomized(RoaringBitmap index, int[] ukeys, DataOutput[] dataOutputs) throws IOException {
        RoaringArray array = index.highLowContainer;
        short[] keys = intToShortKeys(ukeys);
        boolean[] out = new boolean[keys.length];
        for (int i = 0; i < keys.length; i++) {
            if (dataOutputs[i] == null) {
                continue;
            }
            Container container = array.getContainer(keys[i]);
            if (container != null) {
                serializeAtomized(container, dataOutputs[i]);
                out[i] = true;
            } else {
                out[i] = false;
            }
        }
        return out;
    }

    public static short[] serialize(RoaringBitmap bitmap, DataOutput[] outContainers) throws IOException {
        RoaringArray array = bitmap.highLowContainer;

        short[] keys = new short[outContainers.length];
        for (int k = 0; k < array.size; ++k) {
            keys[k] = array.keys[k];
            serializeAtomized(array.values[k], outContainers[k]);
        }
        return keys;
    }

    private static void serializeAtomized(Container value, DataOutput outContainer) throws IOException {
        outContainer.writeShort(lastSetIndex(value));
        outContainer.writeBoolean(value instanceof RunContainer);
        outContainer.writeShort(Short.reverseBytes((short) (value.getCardinality() - 1)));

        value.writeArray(outContainer);
    }

    public static long[] serializeSizeInBytes(RoaringBitmap bitmap, int[] ukeys) {
        RoaringArray array = bitmap.highLowContainer;
        short[] keys = intToShortKeys(ukeys);
        long[] sizes = new long[keys.length];
        for (int i = 0; i < keys.length; i++) {
            Container container = array.getContainer(keys[i]);
            sizes[i] = (container == null) ? -1 : container.serializedSizeInBytes();
        }
        return sizes;
    }

    public static boolean udeserialize(BitmapAndLastId<RoaringBitmap> result, StreamAtoms streamAtoms) throws IOException {
        try {
            List<ContainerAndLastSetBit> containers = Lists.newArrayList();
            boolean v = streamAtoms.stream((key, dataInput) -> {
                containers.add(deserializeContainer(intToShortKey(key), dataInput));
                return true;
            });
            if (containers.isEmpty()) {
                result.clear();
            } else {
                boolean isAscending = true;
                boolean isDescending = true;
                for (int i = 1; i < containers.size(); i++) {
                    short lastKey = containers.get(i - 1).key;
                    short nextKey = containers.get(i).key;
                    if (lastKey > nextKey) {
                        isAscending = false;
                    } else if (lastKey < nextKey) {
                        isDescending = false;
                    }
                }
                if (isAscending) {
                    // do nothing
                } else if (isDescending) {
                    Collections.reverse(containers);
                } else {
                    Collections.sort(containers);
                }
                RoaringBitmap bitmap = new RoaringBitmap();
                int lastSetBit = deserialize(bitmap, containers);
                result.set(bitmap, lastSetBit);
            }
            return v;
        } catch (Exception e) {
            throw new IOException(e);
        }
    }

    public static int lastSetBit(int ukey, DataInput inContainer) throws IOException {
        int last = inContainer.readShort() & 0xFFFF;
        int hs = ukey << 16;
        return last | hs;
    }

    @VisibleForTesting
    static int deserialize(RoaringBitmap bitmap, List<ContainerAndLastSetBit> containers) throws IOException {
        RoaringArray array = bitmap.highLowContainer;

        array.clear();
        array.size = containers.size();

        if ((array.keys == null) || (array.keys.length < array.size)) {
            array.keys = new short[array.size];
            array.values = new Container[array.size];
        }

        int lastSetBit = -1;
        //Reading the containers
        for (int k = 0; k < containers.size(); ++k) {
            ContainerAndLastSetBit container = containers.get(k);
            array.keys[k] = container.key;
            array.values[k] = container.container;
            lastSetBit = Math.max(lastSetBit, container.lastSetBit);
        }
        return lastSetBit;
    }

    private static ContainerAndLastSetBit deserializeContainer(short key, DataInput inContainer) throws IOException {

        int last = inContainer.readShort() & 0xFFFF;
        boolean isRun = inContainer.readBoolean();
        int cardinality = 1 + (0xFFFF & Short.reverseBytes(inContainer.readShort()));
        boolean isBitmap = cardinality > ArrayContainer.DEFAULT_MAX_SIZE;

        int hs = toIntUnsigned(key) << 16;
        int lastSetBit = last | hs;

        Container val;
        if (!isRun && isBitmap) {
            final long[] bitmapArray = new long[BitmapContainer.MAX_CAPACITY / 64];
            // little endian
            for (int l = 0; l < bitmapArray.length; ++l) {
                bitmapArray[l] = Long.reverseBytes(inContainer.readLong());
            }
            val = new BitmapContainer(bitmapArray, cardinality);
        } else if (isRun) {
            // cf RunContainer.writeArray()
            int nbrruns = toIntUnsigned(Short.reverseBytes(inContainer.readShort()));
            final short lengthsAndValues[] = new short[2 * nbrruns];

            for (int j = 0; j < 2 * nbrruns; ++j) {
                lengthsAndValues[j] = Short.reverseBytes(inContainer.readShort());
            }
            val = new RunContainer(lengthsAndValues, nbrruns);
        } else {
            final short[] shortArray = new short[cardinality];
            for (int l = 0; l < shortArray.length; ++l) {
                shortArray[l] = Short.reverseBytes(inContainer.readShort());
            }
            val = new ArrayContainer(shortArray);
        }
        return new ContainerAndLastSetBit(key, val, lastSetBit);
    }

    public static void optimize(RoaringBitmap bitmap, int[] ukeys) {
        RoaringArray array = bitmap.highLowContainer;
        short[] keys = intToShortKeys(ukeys);
        for (int i = 0; i < keys.length; i++) {
            int index = array.getIndex(keys[i]);
            if (index >= 0) {
                Container container = array.getContainerAtIndex(index);
                array.setContainerAtIndex(index, container.runOptimize());
            }
        }
    }

    private static class ContainerAndLastSetBit implements Comparable<ContainerAndLastSetBit> {
        private final short key;
        private final Container container;
        private final int lastSetBit;

        private ContainerAndLastSetBit(short key, Container container, int lastSetBit) {
            this.key = key;
            this.container = container;
            this.lastSetBit = lastSetBit;
        }

        @Override
        public int compareTo(ContainerAndLastSetBit o) {
            return Shorts.compare(key, o.key);
        }
    }

    public static short intToShortKey(int ukey) {
        return Util.lowbits(ukey);
    }

    public static short[] intToShortKeys(int[] ukeys) {
        short[] keys = new short[ukeys.length];
        for (int i = 0; i < keys.length; i++) {
            keys[i] = Util.lowbits(ukeys[i]);
        }
        return keys;
    }

    public static int shortToIntKey(short key) {
        return toIntUnsigned(key);
    }

    public static int[] shortToIntKeys(short[] keys) {
        int[] ukeys = new int[keys.length];
        for (int i = 0; i < ukeys.length; i++) {
            ukeys[i] = toIntUnsigned(keys[i]);
        }
        return ukeys;
    }

    public static int containerCount(RoaringBitmap bitmap) {
        return bitmap.highLowContainer.size;
    }

    public static int naiveFirstIntersectingBit(RoaringBitmap x1, RoaringBitmap x2) {
        int length1 = x1.highLowContainer.size();
        int length2 = x2.highLowContainer.size();
        int pos1 = 0;
        int pos2 = 0;

        while (pos1 < length1 && pos2 < length2) {
            short s1 = x1.highLowContainer.getKeyAtIndex(pos1);
            short s2 = x2.highLowContainer.getKeyAtIndex(pos2);
            if (s1 == s2) {
                Container c1 = x1.highLowContainer.getContainerAtIndex(pos1);
                Container c2 = x2.highLowContainer.getContainerAtIndex(pos2);
                if (c1.intersects(c2)) {
                    Container and = c1.and(c2);
                    int hs = toIntUnsigned(s1) << 16;
                    return and.first() | hs;
                }

                ++pos1;
                ++pos2;
            } else if (Util.compareUnsigned(s1, s2) < 0) {
                pos1 = x1.highLowContainer.advanceUntil(s2, pos1);
            } else {
                pos2 = x2.highLowContainer.advanceUntil(s1, pos2);
            }
        }

        return -1;
    }

    public static int firstIntersectingBit(RoaringBitmap x1, RoaringBitmap x2) {
        int length1 = x1.highLowContainer.size();
        int length2 = x2.highLowContainer.size();
        int pos1 = 0;
        int pos2 = 0;

        while (pos1 < length1 && pos2 < length2) {
            short s1 = x1.highLowContainer.getKeyAtIndex(pos1);
            short s2 = x2.highLowContainer.getKeyAtIndex(pos2);
            if (s1 == s2) {
                Container c1 = x1.highLowContainer.getContainerAtIndex(pos1);
                Container c2 = x2.highLowContainer.getContainerAtIndex(pos2);
                int r = firstIntersectingBit(c1, c2);
                if (r != -1) {
                    int hs = toIntUnsigned(s1) << 16;
                    return r | hs;
                }

                ++pos1;
                ++pos2;
            } else if (Util.compareUnsigned(s1, s2) < 0) {
                pos1 = x1.highLowContainer.advanceUntil(s2, pos1);
            } else {
                pos2 = x2.highLowContainer.advanceUntil(s1, pos2);
            }
        }

        return -1;
    }

    @VisibleForTesting
    static int firstIntersectingBit(Container c1, Container c2) {
        if (c1 instanceof RunContainer) {
            if (c2 instanceof RunContainer) {
                return firstIntersectingBit((RunContainer) c1, (RunContainer) c2);
            } else if (c2 instanceof BitmapContainer) {
                return firstIntersectingBit((RunContainer) c1, (BitmapContainer) c2);
            } else if (c2 instanceof ArrayContainer) {
                return firstIntersectingBit((RunContainer) c1, (ArrayContainer) c2);
            }
        } else if (c1 instanceof BitmapContainer) {
            if (c2 instanceof RunContainer) {
                return firstIntersectingBit((RunContainer) c2, (BitmapContainer) c1);
            } else if (c2 instanceof BitmapContainer) {
                return firstIntersectingBit((BitmapContainer) c1, (BitmapContainer) c2);
            } else if (c2 instanceof ArrayContainer) {
                return firstIntersectingBit((BitmapContainer) c1, (ArrayContainer) c2);
            }
        } else if (c1 instanceof ArrayContainer) {
            if (c2 instanceof RunContainer) {
                return firstIntersectingBit((RunContainer) c2, (ArrayContainer) c1);
            } else if (c2 instanceof BitmapContainer) {
                return firstIntersectingBit((BitmapContainer) c2, (ArrayContainer) c1);
            } else if (c2 instanceof ArrayContainer) {
                return firstIntersectingBit((ArrayContainer) c1, (ArrayContainer) c2);
            }
        }
        return -1;
    }

    private static int firstIntersectingBit(RunContainer c1, RunContainer c2) {
        int rlepos = 0;
        int xrlepos = 0;
        int start = toIntUnsigned(c1.getValue(rlepos));
        int end = start + toIntUnsigned(c1.getLength(rlepos)) + 1;
        int xstart = toIntUnsigned(c2.getValue(xrlepos));
        int xend = xstart + toIntUnsigned(c2.getLength(xrlepos)) + 1;

        while (rlepos < c1.nbrruns && xrlepos < c2.nbrruns) {
            if (end <= xstart) {
                // [start -> end]
                //                [xstart -> xend]
                ++rlepos;
                if (rlepos < c1.nbrruns) {
                    start = toIntUnsigned(c1.getValue(rlepos));
                    end = start + toIntUnsigned(c1.getLength(rlepos)) + 1;
                }
            } else {
                // [start -> end]
                //        [xstart -> xend]

                // [start    ->    end]
                //   [xstart -> xend]

                // [start  ->  end]
                // [xstart -> xend]

                //   [start -> end]
                // [xstart  ->  xend]

                //   [start -> end]
                // [xstart -> xend]

                //                  [start  ->  end]
                // [xstart -> xend]

                if (xend > start) {
                    // [start    ->    end]
                    //   [xstart -> xend]

                    // [start  ->  end]
                    // [xstart -> xend]

                    //   [start  ->  end]
                    // [xstart   ->   xend]

                    //        [start -> end]
                    // [xstart -> xend]
                    return Math.max(start, xstart);
                }

                ++xrlepos;
                if (xrlepos < c2.nbrruns) {
                    xstart = toIntUnsigned(c2.getValue(xrlepos));
                    xend = xstart + toIntUnsigned(c2.getLength(xrlepos)) + 1;
                }
            }
        }

        return -1;
    }

    private static int firstIntersectingBit(RunContainer c1, ArrayContainer c2) {
        if (c1.nbrruns == 0) {
            return -1;
        } else {
            int rlepos = 0;
            int arraypos = 0;
            int rleval = toIntUnsigned(c1.getValue(rlepos));

            for (int rlelength = toIntUnsigned(c1.getLength(rlepos)); arraypos < c2.cardinality; arraypos = Util.advanceUntil(c2.content, arraypos,
                c2.cardinality, c1.getValue(rlepos))) {
                int arrayval;
                for (arrayval = toIntUnsigned(c2.content[arraypos]); rleval + rlelength < arrayval; rlelength = toIntUnsigned(c1.getLength(rlepos))) {
                    ++rlepos;
                    if (rlepos == c1.nbrruns) {
                        return -1;
                    }

                    rleval = toIntUnsigned(c1.getValue(rlepos));
                }

                if (rleval <= arrayval) {
                    return arrayval;
                }
            }

            return -1;
        }
    }

    private static int firstIntersectingBit(RunContainer c1, BitmapContainer c2) {
        for (int rlepos = 0; rlepos < c1.nbrruns; ++rlepos) {
            int runStart = toIntUnsigned(c1.getValue(rlepos));
            int runEnd = runStart + toIntUnsigned(c1.getLength(rlepos));

            for (int runValue = runStart; runValue <= runEnd; ++runValue) {
                if (c2.contains((short) runValue)) {
                    return runValue;
                }
            }
        }

        return -1;
    }

    private static int firstIntersectingBit(BitmapContainer c1, BitmapContainer c2) {
        for (int k = 0; k < c1.bitmap.length; ++k) {
            long x = c1.bitmap[k] & c2.bitmap[k];
            if (x != 0L) {
                return (k * 64) + Long.numberOfTrailingZeros(x);
            }
        }

        return -1;
    }

    private static int firstIntersectingBit(BitmapContainer c1, ArrayContainer c2) {
        int c = c2.cardinality;

        for (int k = 0; k < c; ++k) {
            if (c1.contains(c2.content[k])) {
                return toIntUnsigned(c2.content[k]);
            }
        }

        return -1;
    }

    private static int firstIntersectingBit(ArrayContainer c1, ArrayContainer c2) {
        short[] set1 = c1.content;
        int length1 = c1.getCardinality();
        short[] set2 = c2.content;
        int length2 = c2.getCardinality();
        if (0 != length1 && 0 != length2) {
            int k1 = 0;
            int k2 = 0;
            short s1 = set1[k1];
            short s2 = set2[k2];

            while (true) {
                if (toIntUnsigned(s2) < toIntUnsigned(s1)) {
                    do {
                        ++k2;
                        if (k2 == length2) {
                            return -1;
                        }

                        s2 = set2[k2];
                    }
                    while (toIntUnsigned(s2) < toIntUnsigned(s1));
                }

                if (toIntUnsigned(s1) >= toIntUnsigned(s2)) {
                    return Util.toIntUnsigned(s1);
                }

                while (true) {
                    ++k1;
                    if (k1 == length1) {
                        return -1;
                    }

                    s1 = set1[k1];
                    if (toIntUnsigned(s1) >= toIntUnsigned(s2)) {
                        break;
                    }
                }
            }
        } else {
            return -1;
        }
    }

    private RoaringInspection() {
    }
}
