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
import com.jivesoftware.os.filer.io.ByteBufferDataInput;
import com.jivesoftware.os.filer.io.FilerDataInput;
import com.jivesoftware.os.filer.io.api.StackBuffer;
import com.jivesoftware.os.filer.io.chunk.ChunkFiler;
import com.jivesoftware.os.miru.plugin.bitmap.CardinalityAndLastSetBit;
import com.jivesoftware.os.miru.plugin.bitmap.MiruBitmaps;
import com.jivesoftware.os.miru.plugin.bitmap.MiruIntIterator;
import com.jivesoftware.os.miru.plugin.index.MiruInvertedIndex;
import com.jivesoftware.os.miru.plugin.index.MiruTimeIndex;
import com.jivesoftware.os.miru.plugin.index.MiruTxIndex;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.IOException;
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
    public RoaringBitmap orTx(List<MiruTxIndex<RoaringBitmap>> indexes, StackBuffer stackBuffer) throws Exception {
        if (indexes.isEmpty()) {
            return new RoaringBitmap();
        }

        RoaringBitmap container = indexes.get(0).txIndex((bitmap, filer, stackBuffer1) -> {
            if (bitmap != null) {
                return bitmap;
            } else if (filer != null) {
                return bitmapFromFiler(filer, stackBuffer1);
            } else {
                return new RoaringBitmap();
            }
        }, stackBuffer);

        for (MiruTxIndex<RoaringBitmap> index : indexes.subList(1, indexes.size())) {
            RoaringBitmap or = index.txIndex((bitmap, filer, stackBuffer1) -> {
                if (bitmap != null) {
                    return bitmap;
                } else if (filer != null) {
                    return bitmapFromFiler(filer, stackBuffer1);
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
    public void inPlaceAnd(RoaringBitmap original, RoaringBitmap bitmap) {
        original.and(bitmap);
    }

    @Override
    public void and(RoaringBitmap container, Collection<RoaringBitmap> bitmaps) {
        RoaringAggregation.and(container, bitmaps.toArray(new RoaringBitmap[bitmaps.size()]));
    }

    @Override
    public RoaringBitmap andTx(List<MiruTxIndex<RoaringBitmap>> indexes, StackBuffer stackBuffer) throws Exception {
        if (indexes.isEmpty()) {
            return new RoaringBitmap();
        }

        RoaringBitmap container = indexes.get(0).txIndex((bitmap, filer, stackBuffer1) -> {
            if (bitmap != null) {
                return bitmap;
            } else if (filer != null) {
                return bitmapFromFiler(filer, stackBuffer1);
            } else {
                return new RoaringBitmap();
            }
        }, stackBuffer);

        if (container.isEmpty()) {
            return container;
        }

        for (MiruTxIndex<RoaringBitmap> index : indexes.subList(1, indexes.size())) {
            RoaringBitmap and = index.txIndex((bitmap, filer, stackBuffer1) -> {
                if (bitmap != null) {
                    return bitmap;
                } else if (filer != null) {
                    return bitmapFromFiler(filer, stackBuffer1);
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
    public void inPlaceAndNot(RoaringBitmap original, MiruInvertedIndex<RoaringBitmap> not, StackBuffer stackBuffer) throws Exception {
        Optional<RoaringBitmap> index = not.getIndex(stackBuffer);
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
    public RoaringBitmap andNotTx(MiruTxIndex<RoaringBitmap> original, List<MiruTxIndex<RoaringBitmap>> not, StackBuffer stackBuffer) throws Exception {
        if (not.isEmpty()) {
            return new RoaringBitmap();
        }

        RoaringBitmap container = original.txIndex((bitmap, filer, stackBuffer1) -> {
            if (bitmap != null) {
                return bitmap;
            } else if (filer != null) {
                return bitmapFromFiler(filer, stackBuffer1);
            } else {
                return new RoaringBitmap();
            }
        }, stackBuffer);

        if (container.isEmpty()) {
            return container;
        }

        for (MiruTxIndex<RoaringBitmap> index : not) {
            RoaringBitmap andNot = index.txIndex((bitmap, filer, stackBuffer1) -> {
                if (bitmap != null) {
                    return bitmap;
                } else if (filer != null) {
                    return bitmapFromFiler(filer, stackBuffer1);
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
    public RoaringBitmap buildTimeRangeMask(MiruTimeIndex timeIndex, long smallestTimestamp, long largestTimestamp, StackBuffer stackBuffer) {
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

    private RoaringBitmap bitmapFromFiler(ChunkFiler filer, StackBuffer stackBuffer1) throws IOException {
        RoaringBitmap deser = new RoaringBitmap();
        if (filer.canLeakUnsafeByteBuffer()) {
            deser.deserialize(new ByteBufferDataInput(filer.leakUnsafeByteBuffer()));
        } else {
            deser.deserialize(new FilerDataInput(filer, stackBuffer1));
        }
        return deser;
    }
}
