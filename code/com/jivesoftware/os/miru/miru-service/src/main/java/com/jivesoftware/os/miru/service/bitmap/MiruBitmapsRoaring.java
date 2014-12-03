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
package com.jivesoftware.os.miru.service.bitmap;

import com.google.common.base.Optional;
import com.jivesoftware.os.miru.plugin.bitmap.CardinalityAndLastSetBit;
import com.jivesoftware.os.miru.plugin.bitmap.MiruBitmaps;
import com.jivesoftware.os.miru.plugin.bitmap.MiruIntIterator;
import com.jivesoftware.os.miru.plugin.index.MiruTimeIndex;
import java.io.DataInput;
import java.io.DataOutput;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import org.roaringbitmap.IntIterator;
import org.roaringbitmap.RoaringAggregation;
import org.roaringbitmap.RoaringBitmap;
import org.roaringbitmap.RoaringInspection;

/**
 * @author jonathan
 */
public class MiruBitmapsRoaring implements MiruBitmaps<RoaringBitmap> {

    @Override
    public boolean set(RoaringBitmap bitmap, int... indexes) {
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
    public RoaringBitmap setIntermediate(RoaringBitmap bitmap, int... indexes) {
        for (int index : indexes) {
            bitmap.add(index);
        }
        return bitmap;
    }

    @Override
    public boolean isSet(RoaringBitmap bitmap, int i) {
        return bitmap.contains(i);
    }

    @Override
    public void extend(RoaringBitmap bitmap, List<Integer> indexes, int extendToIndex) {
        for (int index : indexes) {
            bitmap.add(index);
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
    public long[] boundedCardinalities(RoaringBitmap container, int[] indexBoundaries) {
        return RoaringInspection.cardinalityInBuckets(container, indexBoundaries);
    }

    @Override
    public RoaringBitmap create() {
        return new RoaringBitmap();
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
    public void or(RoaringBitmap container, Collection<RoaringBitmap> bitmaps) {
        RoaringAggregation.or(container, bitmaps.toArray(new RoaringBitmap[bitmaps.size()]));
    }

    @Override
    public void and(RoaringBitmap container, Collection<RoaringBitmap> bitmaps) {
        RoaringAggregation.and(container, bitmaps.toArray(new RoaringBitmap[bitmaps.size()]));
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
    public CardinalityAndLastSetBit andNotWithCardinalityAndLastSetBit(RoaringBitmap container, RoaringBitmap original, RoaringBitmap not) {
        andNot(container, original, Collections.singletonList(not));
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
    public RoaringBitmap buildTimeRangeMask(MiruTimeIndex timeIndex, long smallestTimestamp, long largestTimestamp) {
        int smallestInclusiveId = timeIndex.smallestExclusiveTimestampIndex(smallestTimestamp);
        int largestExclusiveId = timeIndex.largestInclusiveTimestampIndex(largestTimestamp) + 1;

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
    public int lastSetBit(RoaringBitmap bitmap) {
        MiruIntIterator iterator = intIterator(bitmap);
        int last = -1;
        while (iterator.hasNext()) {
            last = iterator.next();
        }
        return last;
    }
}
