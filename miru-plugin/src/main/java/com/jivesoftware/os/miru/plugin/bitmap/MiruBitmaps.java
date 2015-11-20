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
package com.jivesoftware.os.miru.plugin.bitmap;

import com.google.common.base.Optional;
import com.jivesoftware.os.miru.plugin.index.MiruInvertedIndex;
import com.jivesoftware.os.miru.plugin.index.MiruTimeIndex;
import java.io.DataInput;
import java.io.DataOutput;
import java.util.Collection;
import java.util.List;

/**
 * @author jonathan
 */
public interface MiruBitmaps<BM> {

    BM create();

    BM createWithBits(int... indexes);

    BM[] createArrayOf(int size);

    void append(BM container, BM bitmap, int... indexes);

    void set(BM container, BM bitmap, int... indexes);

    void remove(BM container, BM bitmap, int... indexes);

    boolean isSet(BM bitmap, int index);

    void extend(BM container, BM bitmap, List<Integer> indexes, int extendToIndex);

    void clear(BM bitmap);

    long cardinality(BM bitmap);

    /**
     * Returns cardinalities for the bitmap bounded by the given indexes. The number of cardinalities returned will be 1 less than
     * the number of boundaries, e.g. indexBoundaries { 0, 10, 20, 30 } returns cardinalities for buckets [ 0-9, 10-19, 20-29 ].
     *
     * @param container       the bitmap
     * @param indexBoundaries lower boundary is inclusive, upper boundary is exclusive
     * @return the cardinalities
     */
    void boundedCardinalities(BM container, int[] indexBoundaries, long[] rawWaveform);

    boolean supportsInPlace();

    boolean isEmpty(BM bitmap);

    long sizeInBytes(BM bitmap);

    long sizeInBits(BM bitmap);

    long serializedSizeInBytes(BM bitmap);

    BM deserialize(DataInput dataInput) throws Exception;

    void serialize(BM bitmap, DataOutput dataOutput) throws Exception;

    void inPlaceOr(BM original, BM or);

    void or(BM container, Collection<BM> bitmaps);

    void inPlaceAnd(BM original, BM bitmap);

    void and(BM container, Collection<BM> bitmaps);

    void inPlaceAndNot(BM original, BM not);

    void inPlaceAndNot(BM original, MiruInvertedIndex<BM> not, byte[] primitiveBuffer) throws Exception;

    void andNot(BM container, BM original, BM not);

    void andNot(BM container, BM original, List<BM> not);

    void copy(BM container, BM original);

    CardinalityAndLastSetBit inPlaceAndNotWithCardinalityAndLastSetBit(BM original, BM not);

    CardinalityAndLastSetBit andNotWithCardinalityAndLastSetBit(BM container, BM original, BM not);

    CardinalityAndLastSetBit andWithCardinalityAndLastSetBit(BM container, List<BM> ands);

    void orToSourceSize(BM container, BM source, BM mask);

    void andNotToSourceSize(BM container, BM source, BM mask);

    void andNotToSourceSize(BM container, BM source, List<BM> masks);

    BM buildIndexMask(int largestIndex, Optional<BM> andNotMask);

    BM buildTimeRangeMask(MiruTimeIndex timeIndex, long smallestTimestamp, long largestTimestamp, byte[] primitiveBuffer);

    MiruIntIterator intIterator(BM bitmap);

    MiruIntIterator descendingIntIterator(BM bitmap);

    int[] indexes(BM bitmap);

    int lastSetBit(BM bitmap);

}
