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
package com.jivesoftware.os.miru.bitmaps.roaring5;

import com.google.common.base.Optional;
import com.google.common.collect.HashMultiset;
import com.google.common.collect.Lists;
import com.google.common.collect.Multiset;
import com.google.common.collect.Multiset.Entry;
import com.google.common.collect.Sets;
import com.jivesoftware.os.filer.io.ByteBufferDataInput;
import com.jivesoftware.os.filer.io.Filer;
import com.jivesoftware.os.filer.io.FilerDataInput;
import com.jivesoftware.os.filer.io.FilerIO;
import com.jivesoftware.os.filer.io.api.StackBuffer;
import com.jivesoftware.os.filer.io.chunk.ChunkFiler;
import com.jivesoftware.os.miru.plugin.bitmap.CardinalityAndLastSetBit;
import com.jivesoftware.os.miru.plugin.bitmap.MiruBitmaps;
import com.jivesoftware.os.miru.plugin.bitmap.MiruIntIterator;
import com.jivesoftware.os.miru.plugin.index.IndexAlignedBitmapStream;
import com.jivesoftware.os.miru.plugin.index.MiruInvertedIndex;
import com.jivesoftware.os.miru.plugin.index.MiruMultiTxIndex;
import com.jivesoftware.os.miru.plugin.index.MiruTimeIndex;
import com.jivesoftware.os.miru.plugin.index.MiruTxIndex;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Deque;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import java.util.Set;
import org.roaringbitmap.FastAggregation;
import org.roaringbitmap.IntIterator;
import org.roaringbitmap.RoaringBitmap;
import org.roaringbitmap.RoaringInspection;

/**
 * @author jonathan
 */
public class MiruBitmapsRoaring implements MiruBitmaps<RoaringBitmap, RoaringBitmap> {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    private boolean appendInPlace(RoaringBitmap bitmap, int... indexes) {
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
    public RoaringBitmap append(RoaringBitmap bitmap, int... indexes) {
        RoaringBitmap container = copy(bitmap);
        appendInPlace(container, indexes);
        return container;
    }

    @Override
    public RoaringBitmap set(RoaringBitmap bitmap, int... indexes) {
        RoaringBitmap container = copy(bitmap);
        for (int index : indexes) {
            container.add(index);
        }
        return container;
    }

    @Override
    public RoaringBitmap removeRange(RoaringBitmap original, int rangeStartInclusive, int rangeEndExclusive) {
        return RoaringBitmap.remove(original, rangeStartInclusive, rangeEndExclusive);
    }

    @Override
    public void inPlaceRemoveRange(RoaringBitmap original, int rangeStartInclusive, int rangeEndExclusive) {
        original.remove(rangeStartInclusive, rangeEndExclusive);
    }

    @Override
    public RoaringBitmap remove(RoaringBitmap bitmap, int... indexes) {
        RoaringBitmap container = copy(bitmap);
        for (int index : indexes) {
            container.remove(index);
        }
        return container;
    }

    @Override
    public boolean removeIfPresent(RoaringBitmap bitmap, int index) {
        return bitmap.checkedRemove(index);
    }

    @Override
    public boolean isSet(RoaringBitmap bitmap, int i) {
        return bitmap.contains(i);
    }

    @Override
    public RoaringBitmap extend(RoaringBitmap bitmap, List<Integer> indexes, int extendToIndex) {
        RoaringBitmap container = copy(bitmap);
        for (int index : indexes) {
            container.add(index);
        }
        return container;
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
    public void boundedCardinalities(RoaringBitmap bitmap, int[][] indexBoundaries, long[][] rawWaveforms) {
        RoaringInspection.cardinalityInBuckets(bitmap, indexBoundaries, rawWaveforms);
    }

    @Override
    public RoaringBitmap create() {
        return new RoaringBitmap();
    }

    @Override
    public RoaringBitmap createWithBits(int... indexes) {
        return RoaringBitmap.bitmapOf(indexes);
    }

    @Override
    public RoaringBitmap[] createArrayOf(int size) {
        RoaringBitmap[] bitmaps = new RoaringBitmap[size];
        for (int i = 0; i < size; i++) {
            bitmaps[i] = new RoaringBitmap();
        }
        return bitmaps;
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
    public RoaringBitmap or(Collection<RoaringBitmap> bitmaps) {
        return FastAggregation.or(bitmaps.iterator());
    }

    @Override
    public RoaringBitmap orTx(List<MiruTxIndex<RoaringBitmap>> indexes, StackBuffer stackBuffer) throws Exception {
        if (indexes.isEmpty()) {
            return new RoaringBitmap();
        }

        RoaringBitmap container = indexes.get(0).txIndex((bitmap, filer, offset, stackBuffer1) -> {
            if (bitmap != null) {
                return bitmap;
            } else if (filer != null) {
                return bitmapFromFiler(filer, offset, stackBuffer1);
            } else {
                return new RoaringBitmap();
            }
        }, stackBuffer);

        for (MiruTxIndex<RoaringBitmap> index : indexes.subList(1, indexes.size())) {
            RoaringBitmap or = index.txIndex((bitmap, filer, offset, stackBuffer1) -> {
                if (bitmap != null) {
                    return bitmap;
                } else if (filer != null) {
                    return bitmapFromFiler(filer, offset, stackBuffer1);
                } else {
                    return null;
                }
            }, stackBuffer);

            if (or != null) {
                container.or(or);
            }
        }

        return container;
    }

    @Override
    public RoaringBitmap orMultiTx(MiruMultiTxIndex<RoaringBitmap> multiTermTxIndex, StackBuffer stackBuffer) throws Exception {
        RoaringBitmap container = new RoaringBitmap();
        multiTermTxIndex.txIndex((index, bitmap, filer, offset, stackBuffer1) -> {
            if (bitmap != null) {
                container.or(bitmap);
            } else if (filer != null) {
                container.or(bitmapFromFiler(filer, offset, stackBuffer1));
            }
        }, stackBuffer);
        return container;
    }

    @Override
    public void inPlaceAnd(RoaringBitmap original, RoaringBitmap bitmap) {
        try {
            original.and(bitmap);
        } catch (Throwable t) {
            LOG.error("WTF:" + original.getCardinality() + " inPlaceAnd " + bitmap.getCardinality());
            throw t;
        }
    }

    @Override
    public RoaringBitmap and(Collection<RoaringBitmap> bitmaps) {
        return FastAggregation.and(bitmaps.iterator());
    }

    @Override
    public RoaringBitmap andTx(List<MiruTxIndex<RoaringBitmap>> indexes, StackBuffer stackBuffer) throws Exception {
        if (indexes.isEmpty()) {
            return new RoaringBitmap();
        }

        RoaringBitmap container = indexes.get(0).txIndex((bitmap, filer, offset, stackBuffer1) -> {
            if (bitmap != null) {
                return bitmap;
            } else if (filer != null) {
                return bitmapFromFiler(filer, offset, stackBuffer1);
            } else {
                return new RoaringBitmap();
            }
        }, stackBuffer);

        if (container.isEmpty()) {
            return container;
        }

        for (MiruTxIndex<RoaringBitmap> index : indexes.subList(1, indexes.size())) {
            RoaringBitmap and = index.txIndex((bitmap, filer, offset, stackBuffer1) -> {
                if (bitmap != null) {
                    return bitmap;
                } else if (filer != null) {
                    return bitmapFromFiler(filer, offset, stackBuffer1);
                } else {
                    return new RoaringBitmap();
                }
            }, stackBuffer);

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
    public void inPlaceAndNot(RoaringBitmap original, MiruInvertedIndex<RoaringBitmap, RoaringBitmap> not, StackBuffer stackBuffer) throws Exception {
        Optional<RoaringBitmap> index = not.getIndex(stackBuffer);
        if (index.isPresent()) {
            original.andNot(index.get());
        }
    }

    @Override
    public RoaringBitmap andNotMultiTx(RoaringBitmap original,
        MiruMultiTxIndex<RoaringBitmap> multiTermTxIndex,
        StackBuffer stackBuffer) throws Exception {

        RoaringBitmap container = copy(original);
        inPlaceAndNotMultiTx(container, multiTermTxIndex, stackBuffer);
        return container;
    }

    @Override
    public void inPlaceAndNotMultiTx(RoaringBitmap original, MiruMultiTxIndex<RoaringBitmap> multiTermTxIndex, StackBuffer stackBuffer) throws Exception {
        multiTermTxIndex.txIndex((index, bitmap, filer, offset, stackBuffer1) -> {
            if (bitmap != null) {
                original.andNot(bitmap);
            } else if (filer != null) {
                original.andNot(bitmapFromFiler(filer, offset, stackBuffer1));
            }
        }, stackBuffer);
    }

    @Override
    public void multiTx(MiruMultiTxIndex<RoaringBitmap> multiTermTxIndex,
        IndexAlignedBitmapStream<RoaringBitmap> stream,
        StackBuffer stackBuffer) throws Exception {

        multiTermTxIndex.txIndex((index, bitmap, filer, offset, stackBuffer1) -> {
            if (bitmap != null) {
                stream.stream(index, copy(bitmap));
            } else if (filer != null) {
                stream.stream(index, bitmapFromFiler(filer, offset, stackBuffer1));
            }
        }, stackBuffer);
    }

    @Override
    public RoaringBitmap andNot(RoaringBitmap original, RoaringBitmap bitmap) {
        return RoaringBitmap.andNot(original, bitmap);
    }

    @Override
    public RoaringBitmap andNot(RoaringBitmap original, List<RoaringBitmap> bitmaps) {

        if (bitmaps.isEmpty()) {
            return copy(original);
        } else {
            RoaringBitmap container = RoaringBitmap.andNot(original, bitmaps.get(0));
            for (int i = 1; i < bitmaps.size(); i++) {
                container.andNot(bitmaps.get(i));
                if (container.isEmpty()) {
                    break;
                }
            }
            return container;
        }
    }

    @Override
    public RoaringBitmap andNotTx(MiruTxIndex<RoaringBitmap> original, List<MiruTxIndex<RoaringBitmap>> not, StackBuffer stackBuffer) throws Exception {
        if (not.isEmpty()) {
            return new RoaringBitmap();
        }

        RoaringBitmap container = original.txIndex((bitmap, filer, offset, stackBuffer1) -> {
            if (bitmap != null) {
                return bitmap;
            } else if (filer != null) {
                return bitmapFromFiler(filer, offset, stackBuffer1);
            } else {
                return new RoaringBitmap();
            }
        }, stackBuffer);

        if (container.isEmpty()) {
            return container;
        }

        for (MiruTxIndex<RoaringBitmap> index : not) {
            RoaringBitmap andNot = index.txIndex((bitmap, filer, offset, stackBuffer1) -> {
                if (bitmap != null) {
                    return bitmap;
                } else if (filer != null) {
                    return bitmapFromFiler(filer, offset, stackBuffer1);
                } else {
                    return null;
                }
            }, stackBuffer);

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
    public CardinalityAndLastSetBit<RoaringBitmap> inPlaceAndNotWithCardinalityAndLastSetBit(RoaringBitmap original, RoaringBitmap not) {
        original.andNot(not);
        return RoaringInspection.cardinalityAndLastSetBit(original);
    }

    @Override
    public CardinalityAndLastSetBit<RoaringBitmap> andNotWithCardinalityAndLastSetBit(RoaringBitmap original, RoaringBitmap not) {
        RoaringBitmap container = andNot(original, not);
        return RoaringInspection.cardinalityAndLastSetBit(container);
    }

    @Override
    public CardinalityAndLastSetBit<RoaringBitmap> andWithCardinalityAndLastSetBit(List<RoaringBitmap> ands) {
        RoaringBitmap container = and(ands);
        return RoaringInspection.cardinalityAndLastSetBit(container);
    }

    @Override
    public RoaringBitmap orToSourceSize(RoaringBitmap source, RoaringBitmap mask) {
        return or(Arrays.asList(source, mask));
    }

    @Override
    public RoaringBitmap andNotToSourceSize(RoaringBitmap source, RoaringBitmap mask) {
        return andNot(source, mask);
    }

    @Override
    public RoaringBitmap andNotToSourceSize(RoaringBitmap source, List<RoaringBitmap> masks) {
        return andNot(source, masks);
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
    public RoaringBitmap buildIndexMask(int largestIndex, Optional<? extends RoaringBitmap> andNotMask) {
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
    public RoaringBitmap buildTimeRangeMask(MiruTimeIndex timeIndex, long smallestTimestamp, long largestTimestamp, StackBuffer stackBuffer) throws
        IOException, InterruptedException {
        int smallestInclusiveId = timeIndex.smallestExclusiveTimestampIndex(smallestTimestamp, stackBuffer);
        int largestExclusiveId = timeIndex.largestInclusiveTimestampIndex(largestTimestamp, stackBuffer) + 1;

        RoaringBitmap mask = new RoaringBitmap();

        if (largestExclusiveId < 0 || smallestInclusiveId > largestExclusiveId) {
            return mask;
        }
        mask.flip(smallestInclusiveId, largestExclusiveId);
        return mask;
    }

    @Override
    public RoaringBitmap copy(RoaringBitmap original) {
        RoaringBitmap container = new RoaringBitmap();
        container.or(original);
        return container;
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

    @Override
    public boolean containsAll(RoaringBitmap container, RoaringBitmap contained) {
        RoaringBitmap intersection = RoaringBitmap.and(container, contained);
        return intersection.getCardinality() == contained.getCardinality();
    }

    @Override
    public boolean containsAny(RoaringBitmap container, RoaringBitmap contained) {
        RoaringBitmap intersection = RoaringBitmap.and(container, contained);
        return !intersection.isEmpty();
    }

    private RoaringBitmap bitmapFromFiler(Filer filer, int offset, StackBuffer stackBuffer1) throws IOException {
        RoaringBitmap deser = new RoaringBitmap();
        if (filer instanceof ChunkFiler && ((ChunkFiler) filer).canLeakUnsafeByteBuffer()) {
            ByteBuffer buf = ((ChunkFiler) filer).leakUnsafeByteBuffer();
            buf.position(offset);
            deser.deserialize(new ByteBufferDataInput(buf));
        } else {
            filer.seek(offset);
            deser.deserialize(new FilerDataInput(filer, stackBuffer1));
        }
        return deser;
    }

    public static class Value {

        final byte[] bytes;

        public Value(byte[] bytes) {
            this.bytes = bytes;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            Value value = (Value) o;

            return Arrays.equals(bytes, value.bytes);

        }

        @Override
        public int hashCode() {
            return bytes != null ? Arrays.hashCode(bytes) : 0;
        }
    }

    public static void main(String[] args) throws Exception {
        int[] pieces = {4}; //{ 4, 8, 4, 8 };
        int[] cardinalities = {3_000}; //{ 1, 100, 3, 10 };
        int numInserts = 10_000;

        int numBytes = 0;
        for (int piece : pieces) {
            numBytes += piece;
        }
        int numBits = numBytes * 8;

        Random rand = new Random(1234);

        for (int run = 0; run < 1_000_000; run++) {

            RoaringBitmap[] vertical = new RoaringBitmap[numBits];
            for (int i = 0; i < vertical.length; i++) {
                vertical[i] = new RoaringBitmap();
            }

            System.out.println("-------------------------------");
            System.out.println("Writing...");

            Set<Value> expected = Sets.newHashSet();
            for (int i = 0; i < numInserts; i++) {
                byte[] value = new byte[numBytes];
                int offset = 0;
                for (int j = 0; j < pieces.length; j++) {
                    int piece = pieces[j];
                    int cardinality = cardinalities[j];
                    if (piece == 4) {
                        int p = 1 + rand.nextInt(cardinality);
                        byte[] pb = FilerIO.intBytes(p);
                        System.arraycopy(pb, 0, value, offset, 4);
                    } else if (piece == 8) {
                        long p = 1 + rand.nextInt(cardinality);
                        byte[] pb = FilerIO.longBytes(p);
                        System.arraycopy(pb, 0, value, offset, 8);
                    }
                    offset += piece;
                }

                //rand.nextBytes(value);
                //value[rand.nextInt(numBytes)] |= 1;
                //long value = 1L + (mask & rand.nextInt(100_000));
                //int value = 1 + rand.nextInt(1_000);
                expected.add(new Value(value));

                for (int j = 0; j < numBytes; j++) {
                    int v = value[j] & 0xFF;
                    int bit = j * 8;
                    while (v != 0 && bit < vertical.length) {
                        if ((v & 1) != 0) {
                            vertical[bit].add(i);
                        }
                        bit++;
                        v >>>= 1;
                    }
                }
            }

            System.out.println("Cloning...");

            //RoaringBitmap[] cloned = new RoaringBitmap[vertical.length];
            List<BitmapWithCardinality> bits = Lists.newArrayListWithCapacity(vertical.length);
            for (int i = 0; i < vertical.length; i++) {
                RoaringBitmap clone = vertical[i].clone();
                //cloned[i] = clone;
                bits.add(new BitmapWithCardinality(i, clone));
                //System.out.println("cardinality: " + clone.getCardinality());
            }

            System.out.println("Streaming...");

            /*int traversed = 0;
            int from = 0;
            while (bits[from].isEmpty() && from < bits.length) {
                from++;
            }

            int to = bits.length;
            while (bits[to - 1].isEmpty() && to > 0) {
                to--;
            }*/
            int[] valueBits = new int[numBits];
            int[] bitBytes = new int[numBits];
            for (int i = 0; i < numBits; i++) {
                valueBits[i] = 1 << (i & 0x07);
                bitBytes[i] = i >> 3;
            }

            Multiset<Value> found = HashMultiset.create(expected.size());
            int[] streamed = {0};
            int maxQuickSize = 0;

            long start = System.currentTimeMillis();
            /*RoaringBitmap input = new RoaringBitmap();
            input.add(0, numInserts);
            int _numBytes = numBytes;
            solve(cloned, input, (output, cardinality) -> {
                byte[] value = new byte[_numBytes];
                for (int i = 0; i < output.length; i++) {
                    if (output[i]) {
                        value[bitBytes[i]] |= valueBits[i];
                    }
                }
                found.add(new Value(value));
                streamed[0]++;
                return true;
            });*/
            Collections.sort(bits);
            //Set<Value> quick = Sets.newHashSet();
            while (!bits.isEmpty()) {
                BitmapWithCardinality next = bits.remove(0);
                //boolean last = bits.isEmpty();
                IntIterator iter = next.bitmap.getIntIterator();
                while (iter.hasNext()) {
                    int id = iter.next();
                    byte[] value = new byte[numBytes];
                    value[bitBytes[next.index]] |= valueBits[next.index];
                    for (BitmapWithCardinality bwc : bits) {
                        if (bwc.bitmap.checkedRemove(id)) {
                            bwc.cardinality--;
                            value[bitBytes[bwc.index]] |= valueBits[bwc.index];
                        }
                    }
                    Value v = new Value(value);
                    //if (last || quick.add(v)) {
                    found.add(v);
                    streamed[0]++;
                    //}
                }
                //maxQuickSize = Math.max(maxQuickSize, quick.size());
                //quick.clear();
                Collections.sort(bits);
            }
            /*for (int i = 0; i <= bits.size(); i++) {
                int _i = i;
                iterConsume(null, bits, i, bits.size(), numBytes, value -> {
                    //System.out.println("Got from:" + _i + " value:" + value);
                    found.add(new Value(value));
                    streamed[0]++;
                });
            }*/
            //found = expected;
            long elapsed = System.currentTimeMillis() - start;

            System.out.println("-------------------------------");
            //System.out.println("traversed: " + traversed);
            System.out.println("streamed: " + streamed[0]);
            System.out.println("maxQuickSize: " + maxQuickSize);
            System.out.println("expected: " + expected.size());
            System.out.println("found: " + found.size());
            boolean isExpected = expected.equals(found.elementSet());
            System.out.println("equal: " + isExpected);
            for (Entry<Value> entry : found.entrySet()) {
                System.out.println("count: " + entry.getCount());
            }

            if (!isExpected) {
                System.out.println("-foundContainsExpected: " + found.elementSet().containsAll(expected));
                System.out.println("-expectedContainsFound: " + expected.containsAll(found.elementSet()));
            }
            System.out.println("elapsed: " + elapsed);
            if (!isExpected) {
                throw new AssertionError("Broken");
            }
        }
    }

    private static class BitmapWithCardinality implements Comparable<BitmapWithCardinality> {

        private final int index;
        private final RoaringBitmap bitmap;
        private int cardinality;

        public BitmapWithCardinality(int index, RoaringBitmap bitmap) {
            this.index = index;
            this.bitmap = bitmap;
            this.cardinality = bitmap.getCardinality();
        }

        @Override
        public int compareTo(BitmapWithCardinality o) {
            return Integer.compare(cardinality, o.cardinality);
        }
    }

    static private void solve(RoaringBitmap[] bits, RoaringBitmap candidate, Solution solution) {
        int bitIndex = bits.length - 1;
        while (bitIndex > -1) {
            RoaringBitmap ones = RoaringBitmap.and(candidate, bits[bitIndex]);
            if (!ones.isEmpty()) {
                break;
            }
            bitIndex--;
        }
        if (bitIndex > -1) {
            solve(bits, 0, bitIndex, candidate, new boolean[bits.length], solution);
        }
    }

    static void solve(RoaringBitmap[] bits, int depth, int bitIndex, RoaringBitmap candidate, boolean[] output, Solution solution) {

        RoaringBitmap zero = RoaringBitmap.andNot(candidate, bits[bitIndex]);
        RoaringBitmap ones = RoaringBitmap.and(candidate, bits[bitIndex]);
        if (bitIndex > 0) {
            output[bitIndex] = true;
            solve(bits, depth + 1, bitIndex - 1, ones, output, solution);
        } else if (!ones.isEmpty()) {
            output[bitIndex] = true;
            solution.stream(output, ones.getCardinality());
        }
        if (bitIndex > 0) {
            output[bitIndex] = false;
            solve(bits, depth + 1, bitIndex - 1, zero, output, solution);
        } else if (!zero.isEmpty()) {
            output[bitIndex] = false;
            solution.stream(output, zero.getCardinality());
        }
    }

    interface Solution {

        boolean stream(boolean[] output, int cardinality);
    }

    interface ValueStream {

        void stream(byte[] value) throws Exception;
    }

    /*public static int consume(RoaringBitmap use, RoaringBitmap[] bits, int from, LongStream stream) throws Exception {
        int traversals = 1;
        RoaringBitmap one = use != null ? RoaringBitmap.and(use, bits[from]) : bits[from];
        RoaringBitmap zero = use != null ? RoaringBitmap.andNot(use, bits[from]) : null;
        if (from == bits.length - 1) {
            if (!one.isEmpty()) {
                //System.out.println("Stream One from:" + from + " value:" + (1L << from));
                stream.stream(1L << from);
            }
            if (zero != null && !zero.isEmpty()) {
                //System.out.println("Stream Zero from:" + from + " value:" + (0));
                stream.stream(0L);
            }
        } else {
            if (!one.isEmpty()) {
                long base = 1L << from;
                int _i = from + 1;
                traversals += consume(one, bits, _i, value -> {
                    long glue = base | value;
                    //System.out.println("Pass One from:" + from + " at:" + _i + " base:" + base + " value:" + value + " glue:" + glue);
                    stream.stream(glue);
                });
            }
            if (zero != null && !zero.isEmpty()) {
                int _i = from + 1;
                traversals += consume(zero, bits, _i, value -> {
                    //System.out.println("Pass Zero from:" + from + " at:" + _i + " value:" + value);
                    stream.stream(value);
                });
            }
        }
        if (use != null) {
            bits[from].andNot(use);
        }
        return traversals;
    }*/
    public static class Frame {

        final RoaringBitmap use;
        final int from;
        //final int to;
        final byte[] base;
        //final LongStream stream;
        RoaringBitmap consumed = null;

        public Frame(RoaringBitmap use, int from, /*int to,*/ byte[] base /* LongStream stream*/) {
            this.use = use;
            this.from = from;
            //this.to = to;
            this.base = base;
            //this.stream = stream;
        }
    }

    final static int threshold = 0;

    public static int iterConsume(RoaringBitmap _use,
        List<BitmapWithCardinality> bits,
        int _from,
        int _to,
        int numBytes,
        ValueStream _stream) throws Exception {
        int traversed = 1;
        Deque<Frame> stack = new LinkedList<>();
        stack.add(new Frame(null, _from, /*_to,*/ new byte[numBytes] /*_stream */));
        while (!stack.isEmpty()) {
            Frame frame = stack.getLast();
            RoaringBitmap use = frame.use;
            int from = frame.from;
            //int to = frame.to;
            byte[] base = frame.base;
            //LongStream stream = frame.stream;
            if (frame.consumed != null) {
                if (use != null) {
                    RoaringBitmap fromBitmap = bits.get(from).bitmap;
                    fromBitmap.andNot(frame.consumed);
                }
                stack.removeLast();
            } else if (from == _to) {
                if (use != null) {
                    _stream.stream(base);
                }
                stack.removeLast();
            } else {
                RoaringBitmap fromBitmap = bits.get(from).bitmap;
                RoaringBitmap ones = use != null ? RoaringBitmap.and(use, fromBitmap) : fromBitmap;
                RoaringBitmap zeroes = use;
                if (zeroes != null) {
                    zeroes.andNot(fromBitmap);
                }
                frame.consumed = ones;
                if (from == bits.size() - 1) {
                    if (!ones.isEmpty()) {
                        byte[] value = new byte[numBytes];
                        System.arraycopy(base, 0, value, 0, numBytes);
                        value[from >> 3] |= (1L << (from & 0x07));
                        _stream.stream(value);
                    }
                    if (zeroes != null && !zeroes.isEmpty()) {
                        _stream.stream(base);
                    }
                } else {
                    int numOnes = ones.getCardinality();
                    if (numOnes > 0 && numOnes <= threshold) {
                        IntIterator iter = ones.getIntIterator();
                        while (iter.hasNext()) {
                            int id = iter.next();
                            byte[] value = new byte[numBytes];
                            System.arraycopy(base, 0, value, 0, numBytes);
                            value[from >> 3] |= (1L << (from & 0x07));
                            for (int i = from + 1; i < bits.size(); i++) {
                                if (bits.get(i).bitmap.checkedRemove(id)) {
                                    value[i >> 3] |= (1L << (i & 0x07));
                                }
                            }
                            _stream.stream(value);
                        }
                    } else if (numOnes > threshold) {
                        byte[] nextBase = new byte[numBytes];
                        System.arraycopy(base, 0, nextBase, 0, numBytes);
                        nextBase[from >> 3] |= (1L << (from & 0x07));
                        int _i = from + 1;
                        traversed++;
                        stack.push(new Frame(ones, _i, nextBase));
                    }

                    int numZeroes = zeroes != null ? zeroes.getCardinality() : 0;
                    if (numZeroes > 0 && numZeroes <= threshold) {
                        IntIterator iter = zeroes.getIntIterator();
                        while (iter.hasNext()) {
                            int id = iter.next();
                            byte[] value = new byte[numBytes];
                            System.arraycopy(base, 0, value, 0, numBytes);
                            for (int i = from + 1; i < bits.size(); i++) {
                                if (bits.get(i).bitmap.checkedRemove(id)) {
                                    value[i >> 3] |= (1L << (i & 0x07));
                                }
                            }
                            _stream.stream(value);
                        }
                    } else if (numZeroes > threshold) {
                        int _i = from + 1;
                        traversed++;
                        stack.push(new Frame(zeroes, _i, base));
                    }
                }
            }
        }
        return traversed;
    }

    public static void main0(String[] args) throws Exception {
        RoaringBitmap horizontal = new RoaringBitmap();
        RoaringBitmap[] vertical = new RoaringBitmap[64];
        for (int i = 0; i < vertical.length; i++) {
            vertical[i] = new RoaringBitmap();
        }

        Random rand = new Random(1234);
        for (int i = 0; i < 3_000_000; i++) {
            long value = rand.nextInt(1_000_000);
            int bit = 0;
            while (value != 0) {
                if ((value & 1) != 0) {
                    horizontal.add(i * 64 + bit);
                    vertical[bit].add(i);
                }
                bit++;
                value >>>= 1;
            }
        }

        System.out.println("horizontal sizeInBytes: " + horizontal.serializedSizeInBytes());

        long verticalSizeInBytes = 0;
        for (int i = 0; i < vertical.length; i++) {
            verticalSizeInBytes += vertical[i].serializedSizeInBytes();
        }
        System.out.println("vertical sizeInBytes: " + verticalSizeInBytes);
    }
}
