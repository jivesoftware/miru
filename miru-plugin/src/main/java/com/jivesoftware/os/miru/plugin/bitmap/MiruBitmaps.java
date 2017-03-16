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
import com.jivesoftware.os.filer.io.api.StackBuffer;
import com.jivesoftware.os.miru.plugin.index.BitmapAndLastId;
import com.jivesoftware.os.miru.plugin.index.IndexAlignedBitmapStream;
import com.jivesoftware.os.miru.plugin.index.MiruInvertedIndex;
import com.jivesoftware.os.miru.plugin.index.MiruMultiTxIndex;
import com.jivesoftware.os.miru.plugin.index.MiruTimeIndex;
import com.jivesoftware.os.miru.plugin.index.MiruTxIndex;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Collection;
import java.util.List;

/**
 * @author jonathan
 */
public interface MiruBitmaps<BM extends IBM, IBM> {

    BM create();

    BM createWithBits(int... indexes);

    BM[] createArrayOf(int size);

    IBM[] createImmutableArrayOf(int size);

    BM[][] createMultiArrayOf(int size1, int size2);

    BM set(IBM bitmap, int... indexes);

    BM removeRange(BM original, int rangeStartInclusive, int rangeEndExclusive);

    void inPlaceRemoveRange(BM original, int rangeStartInclusive, int rangeEndExclusive);

    BM remove(IBM bitmap, int... indexes);

    boolean removeIfPresent(BM bitmap, int index);

    boolean isSet(IBM bitmap, int index);

    void clear(BM bitmap);

    long cardinality(IBM bitmap);

    /**
     * Returns cardinalities for the bitmap bounded by the given indexes. The number of cardinalities returned will be 1 less than
     * the number of boundaries, e.g. indexBoundaries { 0, 10, 20, 30 } returns cardinalities for buckets [ 0-9, 10-19, 20-29 ].
     *
     * @param bitmap          the bitmap
     * @param indexBoundaries index aligned boundaries, lower boundary is inclusive, upper boundary is exclusive
     * @param rawWaveform     index aligned destination to write the waveform results
     */
    void boundedCardinalities(IBM bitmap, int[][] indexBoundaries, long[][] rawWaveform);

    boolean isEmpty(IBM bitmap);

    long sizeInBytes(IBM bitmap);

    long sizeInBits(IBM bitmap);

    long serializedSizeInBytes(IBM bitmap);

    BM deserialize(DataInput dataInput) throws Exception;

    void serialize(IBM bitmap, DataOutput dataOutput) throws Exception;

    void inPlaceOr(BM original, IBM or);

    BM or(Collection<IBM> bitmaps);

    BM orTx(List<MiruTxIndex<IBM>> indexes, StackBuffer stackBuffer) throws Exception;

    BM orMultiTx(MiruMultiTxIndex<IBM> multiTermTxIndex, StackBuffer stackBuffer) throws Exception;

    void inPlaceAnd(BM original, IBM bitmap);

    BM and(Collection<IBM> bitmaps);

    BM andTx(List<MiruTxIndex<IBM>> indexes, StackBuffer stackBuffer) throws Exception;

    void inPlaceAndNot(BM original, IBM not);

    void inPlaceAndNot(BM original, MiruInvertedIndex<BM, IBM> not, StackBuffer stackBuffer) throws Exception;

    BM andNotMultiTx(BM original,
        MiruMultiTxIndex<IBM> multiTermTxIndex,
        long[] counts,
        Optional<BM> countEligible,
        StackBuffer stackBuffer) throws Exception;

    void inPlaceAndNotMultiTx(BM original,
        MiruMultiTxIndex<IBM> multiTermTxIndex,
        long[] counts,
        Optional<BM> countEligible,
        StackBuffer stackBuffer) throws Exception;

    void multiTx(MiruMultiTxIndex<IBM> multiTermTxIndex, IndexAlignedBitmapStream<BM> stream, StackBuffer stackBuffer) throws Exception;

    BM andNot(IBM original, IBM not);

    BM andNot(IBM original, List<IBM> not);

    BM andNotTx(MiruTxIndex<IBM> original, List<MiruTxIndex<IBM>> not, StackBuffer stackBuffer) throws Exception;

    BM copy(IBM original);

    BM orToSourceSize(IBM source, IBM mask);

    BM andNotToSourceSize(IBM source, IBM mask);

    BM andNotToSourceSize(IBM source, List<IBM> masks);

    IBM buildIndexMask(int largestIndex, MiruInvertedIndex<BM, IBM> removalIndex, BitmapAndLastId<BM> container, StackBuffer stackBuffer) throws Exception;

    IBM buildIndexMask(int smallestIndex,
        int largestIndex,
        MiruInvertedIndex<BM, IBM> removalIndex,
        BitmapAndLastId<BM> container,
        StackBuffer stackBuffer) throws Exception;

    IBM buildTimeRangeMask(MiruTimeIndex timeIndex, long smallestTimestamp, long largestTimestamp, StackBuffer stackBuffer) throws Exception;

    MiruIntIterator intIterator(IBM bitmap);

    MiruIntIterator descendingIntIterator(IBM bitmap);

    int[] indexes(IBM bitmap);

    int firstSetBit(IBM bitmap);

    int lastSetBit(IBM bitmap);

    int firstIntersectingBit(IBM bitmap, IBM other);

    boolean intersects(IBM bitmap, IBM other);

    boolean containsAll(IBM container, IBM contained);

    boolean containsAny(IBM container, IBM contained);

    BM[] split(BM bitmap);

    BM[] extract(BM bitmap, int[] keys);

    int key(int position);

    int[] keys(IBM mask);

    long[] serializeAtomizedSizeInBytes(IBM index, int[] keys);

    void serializeAtomized(IBM index, int[] keys, DataOutput[] dataOutputs) throws IOException;

    boolean deserializeAtomized(BitmapAndLastId<BM> container, StreamAtoms streamAtoms) throws IOException;

    interface StreamAtoms {
        boolean stream(AtomStream atomStream) throws Exception;
    }

    interface AtomStream {
        boolean stream(int key, DataInput dataInput) throws IOException;
    }

    int lastIdAtomized(DataInput dataInputs, int key) throws IOException;
}
