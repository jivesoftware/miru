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
package com.jivesoftware.os.miru.bitmaps.roaring4;

import com.fasterxml.jackson.databind.util.ByteBufferBackedInputStream;
import com.google.common.base.Optional;
import com.jivesoftware.os.miru.plugin.bitmap.CardinalityAndLastSetBit;
import com.jivesoftware.os.miru.plugin.bitmap.MiruBitmaps;
import com.jivesoftware.os.miru.plugin.bitmap.MiruIntIterator;
import com.jivesoftware.os.miru.plugin.index.MiruInvertedIndex;
import com.jivesoftware.os.miru.plugin.index.MiruTimeIndex;
import com.jivesoftware.os.miru.plugin.index.MiruTxIndex;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import org.roaringbitmap.IntIterator;
import org.roaringbitmap.RoaringAggregation;
import org.roaringbitmap.RoaringBitmap;
import org.roaringbitmap.RoaringInspection;

/**
 * @author jonathan
 */
public class MiruBitmapsRoaring implements MiruBitmaps<RoaringBitmap, RoaringBitmap> {

    private boolean append(RoaringBitmap bitmap, int... indexes) {
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
    public void append(RoaringBitmap container, RoaringBitmap bitmap, int... indexes) {
        copy(container, bitmap);
        append(container, indexes);
    }

    @Override
    public void set(RoaringBitmap container, RoaringBitmap bitmap, int... indexes) {
        copy(container, bitmap);
        for (int index : indexes) {
            container.add(index);
        }
    }

    @Override
    public void remove(RoaringBitmap container, RoaringBitmap bitmap, int... indexes) {
        copy(container, bitmap);
        for (int index : indexes) {
            container.remove(index);
        }
    }

    @Override
    public boolean isSet(RoaringBitmap bitmap, int i) {
        return bitmap.contains(i);
    }

    @Override
    public void extend(RoaringBitmap container, RoaringBitmap bitmap, List<Integer> indexes, int extendToIndex) {
        copy(container, bitmap);
        for (int index : indexes) {
            container.add(index);
        }
    }

    @Override
    public void clear(RoaringBitmap bitmap) {
        bitmap.clear();
    }

    @Override
    public long cardinality(RoaringBitmap bitmap) {
        return bitmap.getCardinality();
    }

    @Override
    public void boundedCardinalities(RoaringBitmap container, int[] indexBoundaries, long[] rawWaveform) {
        RoaringInspection.cardinalityInBuckets(container, indexBoundaries, rawWaveform);
    }

    @Override
    public RoaringBitmap create() {
        return new RoaringBitmap();
    }

    @Override
    public RoaringBitmap createWithBits(int... indexes) {
        RoaringBitmap bitmap = new RoaringBitmap();
        append(bitmap, indexes);
        return bitmap;
    }

    @Override
    public RoaringBitmap[] createArrayOf(int size) {
        RoaringBitmap[] ewahs = new RoaringBitmap[size];
        for (int i = 0; i < size; i++) {
            ewahs[i] = new RoaringBitmap();
        }
        return ewahs;
    }

    @Override
    public boolean supportsInPlace() {
        return true;
    }

    @Override
    public void inPlaceOr(RoaringBitmap original, RoaringBitmap or) {
        original.or(or);
    }

    @Override
    public void or(RoaringBitmap container, Collection<RoaringBitmap> bitmaps) {
        RoaringAggregation.or(container, bitmaps.toArray(new RoaringBitmap[bitmaps.size()]));
    }

    @Override
    public RoaringBitmap orTx(List<MiruTxIndex<RoaringBitmap>> indexes, byte[] primitiveBuffer) throws Exception {
        if (indexes.isEmpty()) {
            return new RoaringBitmap();
        }

        RoaringBitmap container = indexes.get(0).txIndex((bitmap, buffer) -> {
            if (bitmap != null) {
                return bitmap;
            } else if (buffer != null) {
                RoaringBitmap deser = new RoaringBitmap();
                deser.deserialize(new DataInputStream(new ByteBufferBackedInputStream(buffer)));
                return deser;
            } else {
                return new RoaringBitmap();
            }
        }, primitiveBuffer);

        for (MiruTxIndex<RoaringBitmap> index : indexes.subList(1, indexes.size())) {
            RoaringBitmap or = index.txIndex((bitmap, buffer) -> {
                if (bitmap != null) {
                    return bitmap;
                } else if (buffer != null) {
                    RoaringBitmap deser = new RoaringBitmap();
                    deser.deserialize(new DataInputStream(new ByteBufferBackedInputStream(buffer)));
                    return deser;
                } else {
                    return null;
                }
            }, primitiveBuffer);

            if (or != null) {
                container.or(or);
            }
        }

        return container;
    }

    @Override
    public void inPlaceAnd(RoaringBitmap original, RoaringBitmap bitmap) {
        original.and(bitmap);
    }

    @Override
    public void and(RoaringBitmap container, Collection<RoaringBitmap> bitmaps) {
        RoaringAggregation.and(container, bitmaps.toArray(new RoaringBitmap[bitmaps.size()]));
    }

    @Override
    public RoaringBitmap andTx(List<MiruTxIndex<RoaringBitmap>> indexes, byte[] primitiveBuffer) throws Exception {
        if (indexes.isEmpty()) {
            return new RoaringBitmap();
        }

        RoaringBitmap container = indexes.get(0).txIndex((bitmap, buffer) -> {
            if (bitmap != null) {
                return bitmap;
            } else if (buffer != null) {
                RoaringBitmap deser = new RoaringBitmap();
                deser.deserialize(new DataInputStream(new ByteBufferBackedInputStream(buffer)));
                return deser;
            } else {
                return new RoaringBitmap();
            }
        }, primitiveBuffer);

        if (container.isEmpty()) {
            return container;
        }

        for (MiruTxIndex<RoaringBitmap> index : indexes.subList(1, indexes.size())) {
            RoaringBitmap and = index.txIndex((bitmap, buffer) -> {
                if (bitmap != null) {
                    return bitmap;
                } else if (buffer != null) {
                    RoaringBitmap deser = new RoaringBitmap();
                    deser.deserialize(new DataInputStream(new ByteBufferBackedInputStream(buffer)));
                    return deser;
                } else {
                    return new RoaringBitmap();
                }
            }, primitiveBuffer);

            container.and(and);

            if (container.isEmpty()) {
                return container;
            }
        }

        return container;
    }

    @Override
    public void inPlaceAndNot(RoaringBitmap original, RoaringBitmap not) {
        original.andNot(not);
    }

    @Override
    public void inPlaceAndNot(RoaringBitmap original, MiruInvertedIndex<RoaringBitmap> not, byte[] primitiveBuffer) throws Exception {
        Optional<RoaringBitmap> index = not.getIndex(primitiveBuffer);
        if (index.isPresent()) {
            original.andNot(index.get());
        }
    }

    @Override
    public void andNot(RoaringBitmap container, RoaringBitmap original, RoaringBitmap bitmap) {
        RoaringAggregation.andNot(container, original, bitmap);
    }

    @Override
    public void andNot(RoaringBitmap container, RoaringBitmap original, List<RoaringBitmap> bitmaps) {

        if (bitmaps.isEmpty()) {
            copy(container, original);
        } else {
            RoaringAggregation.andNot(container, original, bitmaps.get(0));
            for (int i = 1; i < bitmaps.size(); i++) {
                container.andNot(bitmaps.get(i));
                if (container.isEmpty()) {
                    break;
                }
            }
        }
    }

    @Override
    public RoaringBitmap andNotTx(MiruTxIndex<RoaringBitmap> original, List<MiruTxIndex<RoaringBitmap>> not, byte[] primitiveBuffer) throws Exception {
        if (not.isEmpty()) {
            return new RoaringBitmap();
        }

        RoaringBitmap container = original.txIndex((bitmap, buffer) -> {
            if (bitmap != null) {
                return bitmap;
            } else if (buffer != null) {
                RoaringBitmap deser = new RoaringBitmap();
                deser.deserialize(new DataInputStream(new ByteBufferBackedInputStream(buffer)));
                return deser;
            } else {
                return new RoaringBitmap();
            }
        }, primitiveBuffer);

        if (container.isEmpty()) {
            return container;
        }

        for (MiruTxIndex<RoaringBitmap> index : not) {
            RoaringBitmap andNot = index.txIndex((bitmap, buffer) -> {
                if (bitmap != null) {
                    return bitmap;
                } else if (buffer != null) {
                    RoaringBitmap deser = new RoaringBitmap();
                    deser.deserialize(new DataInputStream(new ByteBufferBackedInputStream(buffer)));
                    return deser;
                } else {
                    return null;
                }
            }, primitiveBuffer);

            if (andNot != null) {
                container.andNot(andNot);
            }

            if (container.isEmpty()) {
                return container;
            }
        }

        return container;
    }

    @Override
    public CardinalityAndLastSetBit inPlaceAndNotWithCardinalityAndLastSetBit(RoaringBitmap original, RoaringBitmap not) {
        original.andNot(not);
        return RoaringInspection.cardinalityAndLastSetBit(original);
    }

    @Override
    public CardinalityAndLastSetBit andNotWithCardinalityAndLastSetBit(RoaringBitmap container, RoaringBitmap original, RoaringBitmap not) {
        andNot(container, original, not);
        return RoaringInspection.cardinalityAndLastSetBit(container);
    }

    @Override
    public CardinalityAndLastSetBit andWithCardinalityAndLastSetBit(RoaringBitmap container, List<RoaringBitmap> ands) {
        and(container, ands);
        return RoaringInspection.cardinalityAndLastSetBit(container);
    }

    @Override
    public void orToSourceSize(RoaringBitmap container, RoaringBitmap source, RoaringBitmap mask) {
        or(container, Arrays.asList(source, mask));
    }

    @Override
    public void andNotToSourceSize(RoaringBitmap container, RoaringBitmap source, RoaringBitmap mask) {
        andNot(container, source, mask);
    }

    @Override
    public void andNotToSourceSize(RoaringBitmap container, RoaringBitmap source, List<RoaringBitmap> masks) {
        andNot(container, source, masks);
    }

    @Override
    public RoaringBitmap deserialize(DataInput dataInput) throws Exception {
        RoaringBitmap bitmap = new RoaringBitmap();
        bitmap.deserialize(dataInput);
        return bitmap;
    }

    @Override
    public void serialize(RoaringBitmap bitmap, DataOutput dataOutput) throws Exception {
        bitmap.serialize(dataOutput);
    }

    @Override
    public boolean isEmpty(RoaringBitmap bitmap) {
        return bitmap.isEmpty();
    }

    @Override
    public long sizeInBytes(RoaringBitmap bitmap) {
        return bitmap.getSizeInBytes();
    }

    @Override
    public long sizeInBits(RoaringBitmap bitmap) {
        return RoaringInspection.sizeInBits(bitmap);
    }

    @Override
    public long serializedSizeInBytes(RoaringBitmap bitmap) {
        return bitmap.serializedSizeInBytes();
    }

    @Override
    public RoaringBitmap buildIndexMask(int largestIndex, Optional<RoaringBitmap> andNotMask) {
        RoaringBitmap mask = new RoaringBitmap();
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
    public RoaringBitmap buildTimeRangeMask(MiruTimeIndex timeIndex, long smallestTimestamp, long largestTimestamp, byte[] primitiveBuffer) {
        int smallestInclusiveId = timeIndex.smallestExclusiveTimestampIndex(smallestTimestamp, primitiveBuffer);
        int largestExclusiveId = timeIndex.largestInclusiveTimestampIndex(largestTimestamp, primitiveBuffer) + 1;

        RoaringBitmap mask = new RoaringBitmap();

        if (largestExclusiveId < 0 || smallestInclusiveId > largestExclusiveId) {
            return mask;
        }
        mask.flip(smallestInclusiveId, largestExclusiveId);
        return mask;
    }

    @Override
    public void copy(RoaringBitmap container, RoaringBitmap original) {
        container.or(original);
    }

    @Override
    public MiruIntIterator intIterator(RoaringBitmap bitmap) {
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
    public MiruIntIterator descendingIntIterator(RoaringBitmap bitmap) {
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
    public int[] indexes(RoaringBitmap bitmap) {
        return bitmap.toArray();
    }

    @Override
    public int lastSetBit(RoaringBitmap bitmap) {
        MiruIntIterator iterator = intIterator(bitmap);
        int last = -1;
        while (iterator.hasNext()) {
            last = iterator.next();
        }
        return last;
    }

    /*public static void main(String[] args) throws Exception {
        FileReader fileReader = new FileReader("/Users/kevin.karpenske/Desktop/roaring-xor.out");
        BufferedReader buf = new BufferedReader(fileReader);

        String line1;
        do {
            line1 = buf.readLine();
        }
        while (line1 == null || line1.trim().isEmpty());

        String line2;
        do {
            line2 = buf.readLine();
        }
        while (line2 == null || line2.trim().isEmpty());

        String[] line1split = line1.split(",");
        String[] line2split = line2.split(",");

        int[] indexes1 = new int[line1split.length];
        int[] indexes2 = new int[line2split.length];

        for (int i = 0; i < indexes1.length; i++) {
            indexes1[i] = Integer.parseInt(line1split[i]);
        }

        for (int i = 0; i < indexes2.length; i++) {
            indexes2[i] = Integer.parseInt(line2split[i]);
        }

        System.out.println("1: " + indexes1.length);
        System.out.println("2: " + indexes2.length);


        MiruBitmapsRoaring bitmaps = new MiruBitmapsRoaring();

        RoaringBitmap bitmap1 = new RoaringBitmap();
        bitmaps.append(bitmap1, new RoaringBitmap(), indexes1);
        ByteArrayDataOutput dataOutput = ByteStreams.newDataOutput();
        bitmaps.serialize(bitmap1, dataOutput);
        bitmap1 = bitmaps.deserialize(ByteStreams.newDataInput(dataOutput.toByteArray()));

        int run = 0;
        for (int i = 0; i < indexes2.length; i++) {
            if (i > 0) {
                if (indexes2[i - 1] == indexes2[i] - 1) {
                    run++;
                } else {
                    if (run > 1000) {
                        System.out.println("run of " + run);
                    }
                    run = 0;
                }
            }
        }

        RoaringBitmap bitmap2 = new RoaringBitmap();
        bitmaps.append(bitmap2, new RoaringBitmap(), indexes2);
        dataOutput = ByteStreams.newDataOutput();
        bitmaps.serialize(bitmap2, dataOutput);
        bitmap2 = bitmaps.deserialize(ByteStreams.newDataInput(dataOutput.toByteArray()));

        RoaringBitmap container = new RoaringBitmap();
        RoaringAggregation.or(container, new RoaringBitmap[] { bitmap1, bitmap2 });
        System.out.println("c1: " + container.getCardinality());

        container = new RoaringBitmap();
        RoaringAggregation.or(container, bitmap1, bitmap2);
        System.out.println("c2: " + container.getCardinality());
    }*/

 /*public static void main(String[] args) throws Exception {
        RoaringBitmap bitmap = new RoaringBitmap();
        int[] indexes = { 343798, 343799, 343800, 343801, 343803, 343804, 343805, 343807, 343809, 343811, 343812, 343815, 343816, 343817, 343818, 343819,
            343821, 343825, 343827, 343828, 343830, 343831, 343832, 343833, 343835, 343836, 343837, 343838,
            343839, 343840, 343841, 343842, 343843, 343844, 343845, 343847, 343848, 343849, 343850, 343851, 343853, 343854, 343855, 343856, 343858, 343859,
            343860, 343861, 343862, 343863, 343864, 343865, 343866, 343868, 343869, 343874, 343875,
            343877, 343879, 343880, 343881, 343882, 343883, 343887, 343889, 343890, 343891, 343894, 343895, 343898, 343899, 343900, 343901, 343902, 343904,
            343906, 343907, 343908, 343909, 343910, 343911, 343912, 343913, 343914, 343915, 343916,
            343917, 343918, 343919, 343921, 343922, 343923, 343924, 343927, 343928, 343929, 343930, 343931, 343932, 343933, 343934, 343935, 343938, 343939,
            343941, 343942, 343943, 343944, 343945, 343946, 343949, 343951, 343953, 343954, 343955,
            343956, 343958, 343959, 343961, 343962, 343964, 343965, 343966, 343967, 343968, 343969, 343971, 343972, 343973, 343974, 343976, 343978, 343979,
            343981, 343982, 343983, 343985, 343987, 343988, 343989, 343992, 343993, 343994, 343995,
            343996, 343997, 343998, 344000, 344001, 344002, 344003, 344004, 344006, 344008, 344009, 344011, 344012, 344013, 344015, 344017, 344019, 344020,
            344021, 344023, 344025, 344026, 344027, 344028, 344029, 344030, 344031, 344034, 344035,
            344036, 344037, 344038, 344039, 344040, 344042, 344043, 344046, 344047 };

        int rangeStart = 0;
        for (int rangeEnd = 1; rangeEnd < indexes.length; rangeEnd++) {
            if (indexes[rangeEnd - 1] + 1 != indexes[rangeEnd]) {
                if (rangeStart == rangeEnd - 1) {
                    System.out.println("add " + indexes[rangeStart]);
                    bitmap.add(indexes[rangeStart]);
                } else {
                    System.out.println("flip " + indexes[rangeStart] + " to " + (indexes[rangeEnd - 1] + 1));
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
    }*/
}
