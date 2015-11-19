/*
 * Copyright 2014 jivesoftware.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.jivesoftware.os.miru.bitmaps.roaring5.buffer;

import com.google.common.base.Optional;
import com.google.common.io.ByteArrayDataOutput;
import com.google.common.io.ByteStreams;
import com.jivesoftware.os.miru.plugin.bitmap.CardinalityAndLastSetBit;
import com.jivesoftware.os.miru.plugin.bitmap.MiruBitmaps;
import com.jivesoftware.os.miru.plugin.bitmap.MiruIntIterator;
import com.jivesoftware.os.miru.plugin.index.MiruTimeIndex;
import java.io.DataInput;
import java.io.DataOutput;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Random;
import org.roaringbitmap.IntIterator;
import org.roaringbitmap.RoaringBitmap;
import org.roaringbitmap.buffer.MutableRoaringBitmap;
import org.roaringbitmap.buffer.RoaringBufferAggregation;
import org.roaringbitmap.buffer.RoaringBufferInspection;

/**
 * @author jonathan
 */
public class MiruBitmapsRoaringBuffer implements MiruBitmaps<MutableRoaringBitmap> {

    private boolean append(MutableRoaringBitmap bitmap, int... indexes) {
        if (indexes.length == 1) {
            bitmap.add(indexes[0]);
        } else if (indexes.length > 1) {
            int rangeStart = 0;
            for (int rangeEnd = 1; rangeEnd < indexes.length; rangeEnd++) {
                if (indexes[rangeEnd - 1] + 1 != indexes[rangeEnd]) {
                    if (rangeStart == rangeEnd - 1) {
                        bitmap.add(indexes[rangeStart]);
                    } else {
                        bitmap.flip(indexes[rangeStart], indexes[rangeEnd - 1] + 1);
                    }
                    rangeStart = rangeEnd;
                }
            }
            if (rangeStart == indexes.length - 1) {
                bitmap.add(indexes[rangeStart]);
            } else {
                bitmap.flip(indexes[rangeStart], indexes[indexes.length - 1] + 1);
            }
        }
        return true;
    }

    @Override
    public void append(MutableRoaringBitmap container, MutableRoaringBitmap bitmap, int... indexes) {
        copy(container, bitmap);
        append(container, indexes);
    }

    @Override
    public void set(MutableRoaringBitmap container, MutableRoaringBitmap bitmap, int... indexes) {
        copy(container, bitmap);
        for (int index : indexes) {
            container.add(index);
        }
    }

    @Override
    public void remove(MutableRoaringBitmap container, MutableRoaringBitmap bitmap, int... indexes) {
        copy(container, bitmap);
        for (int index : indexes) {
            container.remove(index);
        }
    }

    @Override
    public boolean isSet(MutableRoaringBitmap bitmap, int i) {
        return bitmap.contains(i);
    }

    @Override
    public void extend(MutableRoaringBitmap container, MutableRoaringBitmap bitmap, List<Integer> indexes, int extendToIndex) {
        copy(container, bitmap);
        for (int index : indexes) {
            container.add(index);
        }
    }

    @Override
    public void clear(MutableRoaringBitmap bitmap) {
        bitmap.clear();
    }

    @Override
    public long cardinality(MutableRoaringBitmap bitmap) {
        return bitmap.getCardinality();
    }

    @Override
    public void boundedCardinalities(MutableRoaringBitmap container, int[] indexBoundaries, long[] rawWaveform) {
        RoaringBufferInspection.cardinalityInBuckets(container, indexBoundaries, rawWaveform);
    }

    @Override
    public MutableRoaringBitmap create() {
        return new MutableRoaringBitmap();
    }

    @Override
    public MutableRoaringBitmap createWithBits(int... indexes) {
        MutableRoaringBitmap bitmap = new MutableRoaringBitmap();
        append(bitmap, indexes);
        return bitmap;
    }

    @Override
    public MutableRoaringBitmap[] createArrayOf(int size) {
        MutableRoaringBitmap[] bitmaps = new MutableRoaringBitmap[size];
        for (int i = 0; i < size; i++) {
            bitmaps[i] = new MutableRoaringBitmap();
        }
        return bitmaps;
    }

    @Override
    public boolean supportsInPlace() {
        return true;
    }

    @Override
    public void or(MutableRoaringBitmap container, Collection<MutableRoaringBitmap> bitmaps) {
        RoaringBufferAggregation.or(container, bitmaps.toArray(new MutableRoaringBitmap[bitmaps.size()]));
    }

    @Override
    public void inPlaceAnd(MutableRoaringBitmap original, MutableRoaringBitmap bitmap) {
        original.and(bitmap);
    }

    @Override
    public void and(MutableRoaringBitmap container, Collection<MutableRoaringBitmap> bitmaps) {
        RoaringBufferAggregation.and(container, bitmaps.toArray(new MutableRoaringBitmap[bitmaps.size()]));
    }

    @Override
    public void inPlaceAndNot(MutableRoaringBitmap original, MutableRoaringBitmap not) {
        original.andNot(not);
    }

    @Override
    public void andNot(MutableRoaringBitmap container, MutableRoaringBitmap original, MutableRoaringBitmap bitmap) {
        RoaringBufferAggregation.andNot(container, original, bitmap);
    }

    @Override
    public void andNot(MutableRoaringBitmap container, MutableRoaringBitmap original, List<MutableRoaringBitmap> bitmaps) {

        if (bitmaps.isEmpty()) {
            copy(container, original);
        } else {
            RoaringBufferAggregation.andNot(container, original, bitmaps.get(0));
            for (int i = 1; i < bitmaps.size(); i++) {
                container.andNot(bitmaps.get(i));
                if (container.isEmpty()) {
                    break;
                }
            }
        }
    }

    @Override
    public CardinalityAndLastSetBit inPlaceAndNotWithCardinalityAndLastSetBit(MutableRoaringBitmap original, MutableRoaringBitmap not) {
        original.andNot(not);
        return RoaringBufferInspection.cardinalityAndLastSetBit(original);
    }

    @Override
    public CardinalityAndLastSetBit andNotWithCardinalityAndLastSetBit(MutableRoaringBitmap container,
        MutableRoaringBitmap original,
        MutableRoaringBitmap not) {
        andNot(container, original, not);
        return RoaringBufferInspection.cardinalityAndLastSetBit(container);
    }

    @Override
    public CardinalityAndLastSetBit andWithCardinalityAndLastSetBit(MutableRoaringBitmap container, List<MutableRoaringBitmap> ands) {
        and(container, ands);
        return RoaringBufferInspection.cardinalityAndLastSetBit(container);
    }

    @Override
    public void orToSourceSize(MutableRoaringBitmap container, MutableRoaringBitmap source, MutableRoaringBitmap mask) {
        or(container, Arrays.asList(source, mask));
    }

    @Override
    public void andNotToSourceSize(MutableRoaringBitmap container, MutableRoaringBitmap source, MutableRoaringBitmap mask) {
        andNot(container, source, mask);
    }

    @Override
    public void andNotToSourceSize(MutableRoaringBitmap container, MutableRoaringBitmap source, List<MutableRoaringBitmap> masks) {
        andNot(container, source, masks);
    }

    @Override
    public MutableRoaringBitmap deserialize(DataInput dataInput) throws Exception {
        MutableRoaringBitmap bitmap = new MutableRoaringBitmap();
        bitmap.deserialize(dataInput);
        return bitmap;
    }

    @Override
    public void serialize(MutableRoaringBitmap bitmap, DataOutput dataOutput) throws Exception {
        bitmap.serialize(dataOutput);
    }

    @Override
    public boolean isEmpty(MutableRoaringBitmap bitmap) {
        return bitmap.isEmpty();
    }

    @Override
    public long sizeInBytes(MutableRoaringBitmap bitmap) {
        return bitmap.getSizeInBytes();
    }

    @Override
    public long sizeInBits(MutableRoaringBitmap bitmap) {
        return RoaringBufferInspection.sizeInBits(bitmap);
    }

    @Override
    public long serializedSizeInBytes(MutableRoaringBitmap bitmap) {
        return bitmap.serializedSizeInBytes();
    }

    @Override
    public MutableRoaringBitmap buildIndexMask(int largestIndex, Optional<MutableRoaringBitmap> andNotMask) {
        MutableRoaringBitmap mask = new MutableRoaringBitmap();
        if (largestIndex < 0) {
            return mask;
        }

        mask.flip(0, largestIndex + 1);
        if (andNotMask.isPresent()) {
            mask.andNot(andNotMask.get());
        }
        return mask;
    }

    @Override
    public MutableRoaringBitmap buildTimeRangeMask(MiruTimeIndex timeIndex, long smallestTimestamp, long largestTimestamp) {
        int smallestInclusiveId = timeIndex.smallestExclusiveTimestampIndex(smallestTimestamp);
        int largestExclusiveId = timeIndex.largestInclusiveTimestampIndex(largestTimestamp) + 1;

        MutableRoaringBitmap mask = new MutableRoaringBitmap();

        if (largestExclusiveId < 0 || smallestInclusiveId > largestExclusiveId) {
            return mask;
        }
        mask.flip(smallestInclusiveId, largestExclusiveId);
        return mask;
    }

    @Override
    public void copy(MutableRoaringBitmap container, MutableRoaringBitmap original) {
        container.or(original);
    }

    @Override
    public MiruIntIterator intIterator(MutableRoaringBitmap bitmap) {
        final IntIterator intIterator = bitmap.getIntIterator();
        return new MiruIntIterator() {

            @Override
            public boolean hasNext() {
                return intIterator.hasNext();
            }

            @Override
            public int next() {
                return intIterator.next();
            }
        };
    }

    @Override
    public MiruIntIterator descendingIntIterator(MutableRoaringBitmap bitmap) {
        final IntIterator intIterator = bitmap.getReverseIntIterator();
        return new MiruIntIterator() {

            @Override
            public boolean hasNext() {
                return intIterator.hasNext();
            }

            @Override
            public int next() {
                return intIterator.next();
            }
        };
    }

    @Override
    public int[] indexes(MutableRoaringBitmap bitmap) {
        return bitmap.toArray();
    }

    @Override
    public int lastSetBit(MutableRoaringBitmap bitmap) {
        MiruIntIterator iterator = intIterator(bitmap);
        int last = -1;
        while (iterator.hasNext()) {
            last = iterator.next();
        }
        return last;
    }

    public static void main(String[] args) throws Exception {

        for (int i = 0; i < 100; i++) {

            RoaringBitmap bitmap = new RoaringBitmap();
            Random r = new Random(1234);
            for (int j = 0; j < 1_000_000; j++) {
                if (r.nextBoolean()) {
                    bitmap.add(j);
                }
            }

            ByteArrayDataOutput out = ByteStreams.newDataOutput();
            bitmap.serialize(out);

            MutableRoaringBitmap buf = new MutableRoaringBitmap();
            buf.deserialize(ByteStreams.newDataInput(out.toByteArray()));

            long start = System.currentTimeMillis();
            IntIterator iter = bitmap.getIntIterator();
            int count = 0;
            while (iter.hasNext()) {
                int next = iter.next();
                RoaringBitmap not = RoaringBitmap.bitmapOf(next);
                bitmap.andNot(not);
                iter = bitmap.getIntIterator();
                count++;
            }
            System.out.println("---- regular ----");
            System.out.println((System.currentTimeMillis() - start) + " ms, " + count + " iter");

            start = System.currentTimeMillis();
            iter = buf.getIntIterator();
            count = 0;
            while (iter.hasNext()) {
                int next = iter.next();
                MutableRoaringBitmap not = MutableRoaringBitmap.bitmapOf(next);
                buf.andNot(not);
                iter = buf.getIntIterator();
                count++;
            }
            System.out.println("---- buffers ----");
            System.out.println((System.currentTimeMillis() - start) + " ms, " + count + " iter");
        }

        /*for (int i = 0; i < 100; i++) {

            long start = System.currentTimeMillis();
            RoaringBitmap b1 = new RoaringBitmap();
            RoaringBitmap b2 = new RoaringBitmap();
            Random r = new Random(1234);
            for (int j = 0; j < 100_000; j++) {
                if (r.nextBoolean()) {
                    b1.add(j);
                }
                if (r.nextBoolean()) {
                    b2.add(j);
                }
                RoaringBitmap b3 = new RoaringBitmap();
                RoaringAggregation.or(b3, b1, b2);
            }
            System.out.println("---- regular ----");
            System.out.println((System.currentTimeMillis() - start) + " ms");

            start = System.currentTimeMillis();
            MutableRoaringBitmap m1 = new MutableRoaringBitmap();
            MutableRoaringBitmap m2 = new MutableRoaringBitmap();
            r = new Random(1234);
            for (int j = 0; j < 100_000; j++) {
                if (r.nextBoolean()) {
                    m1.add(j);
                }
                if (r.nextBoolean()) {
                    m2.add(j);
                }
                MutableRoaringBitmap m3 = BufferFastAggregation.or(m1, m2);
            }
            System.out.println("---- buffers ----");
            System.out.println((System.currentTimeMillis() - start) + " ms");

        }*/
    }
}
