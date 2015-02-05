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
import com.googlecode.javaewah.BitmapStorage;
import com.googlecode.javaewah.EWAHCompressedBitmap;
import com.googlecode.javaewah.FastAggregation;
import com.googlecode.javaewah.IntIterator;
import com.jivesoftware.os.miru.plugin.bitmap.CardinalityAndLastSetBit;
import com.jivesoftware.os.miru.plugin.bitmap.MiruBitmaps;
import com.jivesoftware.os.miru.plugin.bitmap.MiruIntIterator;
import com.jivesoftware.os.miru.plugin.index.MiruTimeIndex;
import java.io.DataInput;
import java.io.DataOutput;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;

/**
 * @author jonathan
 */
public class MiruBitmapsEWAH implements MiruBitmaps<EWAHCompressedBitmap> {
    private static final EWAHCompressedBitmap EMPTY = new EWAHCompressedBitmap(); // Balls!!

    private final int bufferSize;

    public MiruBitmapsEWAH(int bufferSize) {
        this.bufferSize = bufferSize;
    }

    @Override
    public void append(EWAHCompressedBitmap container, EWAHCompressedBitmap bitmap, int... indexes) {
        //TODO we could optimize by adding ranges via streams of words
        set(container, bitmap, indexes);
    }

    @Override
    public void set(EWAHCompressedBitmap container, EWAHCompressedBitmap bitmap, int... indexes) {
        copy(container, bitmap);

        for (int index : indexes) {
            container.set(index);
        }
    }

    @Override
    public void remove(EWAHCompressedBitmap container, EWAHCompressedBitmap bitmap, int index) {
        andNot(container, bitmap, Arrays.asList(createWithBits(index)));
    }

    @Override
    public boolean isSet(EWAHCompressedBitmap bitmap, int i) {
        return bitmap.get(i);
    }

    @Override
    public void extend(EWAHCompressedBitmap container, EWAHCompressedBitmap bitmap, List<Integer> indexes, int extendToIndex) {
        copy(container, bitmap);

        if (indexes.isEmpty() && container.sizeInBits() == extendToIndex + 1) {
            return;
        }
        for (int index : indexes) {
            if (!container.set(index)) {
                throw new RuntimeException("id must be in increasing order"
                    + ", index = " + index
                    + ", cardinality = " + container.cardinality()
                    + ", size in bits = " + container.sizeInBits());
            }
        }
        container.setSizeInBits(extendToIndex, false);
    }

    @Override
    public void clear(EWAHCompressedBitmap bitmap) {
        bitmap.clear();
    }

    @Override
    public long cardinality(EWAHCompressedBitmap bitmap) {
        return bitmap.cardinality();
    }

    @Override
    public long[] boundedCardinalities(EWAHCompressedBitmap container, int[] indexBoundaries) {
        //TODO naive implementation can just walk IntIterator iter = container.intIterator();
        throw new UnsupportedOperationException("Not yet!");
    }

    @Override
    public EWAHCompressedBitmap create() {
        return new EWAHCompressedBitmap();
    }

    @Override
    public EWAHCompressedBitmap createWithBits(int... indexes) {
        EWAHCompressedBitmap bitmap = new EWAHCompressedBitmap();
        for (int index : indexes) {
            bitmap.set(index);
        }
        return bitmap;
    }

    @Override
    public EWAHCompressedBitmap[] createArrayOf(int size) {
        EWAHCompressedBitmap[] ewahs = new EWAHCompressedBitmap[size];
        for (int i = 0; i < size; i++) {
            ewahs[i] = new EWAHCompressedBitmap();
        }
        return ewahs;
    }

    @Override
    public void or(EWAHCompressedBitmap container, Collection<EWAHCompressedBitmap> bitmaps) {
        FastAggregation.bufferedorWithContainer(container, bufferSize, bitmaps.toArray(new EWAHCompressedBitmap[bitmaps.size()]));
    }

    @Override
    public void and(EWAHCompressedBitmap container, Collection<EWAHCompressedBitmap> bitmaps) {
        FastAggregation.bufferedandWithContainer(container, bufferSize, bitmaps.toArray(new EWAHCompressedBitmap[bitmaps.size()]));
    }

    @Override
    public void andNot(EWAHCompressedBitmap container, EWAHCompressedBitmap original, List<EWAHCompressedBitmap> bitmaps) {

        if (bitmaps.isEmpty()) {
            copy(container, original);
        } else if (bitmaps.size() == 1) {
            original.andNotToContainer(bitmaps.get(0), container);
        } else {
            //TODO we've determined that this is less efficient than iteratively performing the andNots, but leaving for a rainy day
            EWAHCompressedBitmap ored = new EWAHCompressedBitmap();
            or((BitmapStorage) ored, bitmaps);
            original.andNotToContainer(ored, container);
        }
    }

    // TODO eval if this is better than FastAgg OR?
    private void or(final BitmapStorage container,
        final List<EWAHCompressedBitmap> bitmaps) {
        if (bitmaps.size() < 2) {
            throw new IllegalArgumentException("We need at least two bitmaps");
        }
        PriorityQueue<EWAHCompressedBitmap> pq = new PriorityQueue<>(bitmaps.size(),
            new Comparator<EWAHCompressedBitmap>() {
                @Override
                public int compare(EWAHCompressedBitmap a, EWAHCompressedBitmap b) {
                    return a.sizeInBytes() - b.sizeInBytes();
                }
            });
        for (EWAHCompressedBitmap x : bitmaps) {
            pq.add(x);
        }
        while (pq.size() > 2) {
            EWAHCompressedBitmap x1 = pq.poll();
            EWAHCompressedBitmap x2 = pq.poll();
            pq.add(x1.or(x2));
        }
        pq.poll().orToContainer(pq.poll(), container);
    }

    @Override
    public CardinalityAndLastSetBit andNotWithCardinalityAndLastSetBit(EWAHCompressedBitmap container, EWAHCompressedBitmap original,
        EWAHCompressedBitmap not) {
        AnswerCardinalityLastSetBitmapStorage storage = new AnswerCardinalityLastSetBitmapStorage(container);
        original.andNotToContainer(not, storage);
        return new CardinalityAndLastSetBit(storage.getCount(), storage.getLastSetBit());
    }

    @Override
    public CardinalityAndLastSetBit andWithCardinalityAndLastSetBit(EWAHCompressedBitmap container, List<EWAHCompressedBitmap> ands) {
        AnswerCardinalityLastSetBitmapStorage storage = new AnswerCardinalityLastSetBitmapStorage(container);
        FastAggregation.bufferedandWithContainer(storage, bufferSize, ands.toArray(new EWAHCompressedBitmap[ands.size()]));
        return new CardinalityAndLastSetBit(storage.getCount(), storage.getLastSetBit());
    }

    @Override
    public void orToSourceSize(EWAHCompressedBitmap container, EWAHCompressedBitmap source, EWAHCompressedBitmap mask) {
        MatchNoMoreThanNBitmapStorage matchNoMoreThanNBitmapStorage = new MatchNoMoreThanNBitmapStorage(container, source.sizeInBits());
        source.orToContainer(mask, matchNoMoreThanNBitmapStorage);
    }

    @Override
    public void andNotToSourceSize(EWAHCompressedBitmap container, EWAHCompressedBitmap source, List<EWAHCompressedBitmap> masks) {

        if (masks.isEmpty()) {
            copy(container, source);
        } else if (masks.size() == 1) {
            MatchNoMoreThanNBitmapStorage matchNoMoreThanNBitmapStorage = new MatchNoMoreThanNBitmapStorage(container, source.sizeInBits());
            source.andNotToContainer(masks.get(0), matchNoMoreThanNBitmapStorage);
        } else {
            //TODO we've determined that this is less efficient than iteratively performing the andNots, but leaving for a rainy day
            EWAHCompressedBitmap ored = new EWAHCompressedBitmap();
            or((BitmapStorage) ored, masks);
            MatchNoMoreThanNBitmapStorage matchNoMoreThanNBitmapStorage = new MatchNoMoreThanNBitmapStorage(container, source.sizeInBits());
            source.andNotToContainer(ored, matchNoMoreThanNBitmapStorage);
        }
    }

    @Override
    public EWAHCompressedBitmap deserialize(DataInput dataInput) throws Exception {
        EWAHCompressedBitmap bitmap = new EWAHCompressedBitmap();
        bitmap.deserialize(dataInput);
        return bitmap;
    }

    @Override
    public void serialize(EWAHCompressedBitmap bitmap, DataOutput dataOutput) throws Exception {
        bitmap.serialize(dataOutput);
    }

    @Override
    public boolean isEmpty(EWAHCompressedBitmap bitmap) {
        return bitmap.isEmpty();
    }

    @Override
    public long sizeInBytes(EWAHCompressedBitmap bitmap) {
        return bitmap.sizeInBytes();
    }

    @Override
    public long sizeInBits(EWAHCompressedBitmap bitmap) {
        return bitmap.sizeInBits();
    }

    @Override
    public long serializedSizeInBytes(EWAHCompressedBitmap bitmap) {
        return bitmap.serializedSizeInBytes();
    }

    @Override
    public EWAHCompressedBitmap buildIndexMask(int largestIndex, Optional<EWAHCompressedBitmap> andNotMask) {
        EWAHCompressedBitmap mask = new EWAHCompressedBitmap();
        if (largestIndex < 0) {
            return mask;
        }

        int words = largestIndex / EWAHCompressedBitmap.WORD_IN_BITS;
        if (words > 0) {
            mask.addStreamOfEmptyWords(true, words);
        }

        int remainingBits = largestIndex % EWAHCompressedBitmap.WORD_IN_BITS + 1;
        long lastWord = 0;
        for (int i = 0; i < remainingBits; i++) {
            lastWord |= (1l << i);
        }

        // include bitsThatMatter so we don't push our pointer to the next word alignment
        mask.addWord(lastWord, remainingBits);

        if (andNotMask.isPresent() && mask.sizeInBits() > 0 && andNotMask.get().sizeInBits() > 0) {
            return mask.andNot(andNotMask.get());
        } else {
            return mask;
        }
    }

    @Override
    public EWAHCompressedBitmap buildTimeRangeMask(MiruTimeIndex timeIndex, long smallestTimestamp, long largestTimestamp) {
        int smallestId = timeIndex.smallestExclusiveTimestampIndex(smallestTimestamp);
        int largestId = timeIndex.largestInclusiveTimestampIndex(largestTimestamp);

        EWAHCompressedBitmap mask = new EWAHCompressedBitmap();

        if (largestId < 0 || smallestId > largestId) {
            return mask;
        }

        int initialZeroWords = smallestId / EWAHCompressedBitmap.WORD_IN_BITS;
        if (initialZeroWords > 0) {
            mask.addStreamOfEmptyWords(false, initialZeroWords);
        }

        //TODO[LP] see if there's a way to simplify this logic
        if (largestId == smallestId) {
            // one bit to set
            mask.set(smallestId);
        } else if (largestId < (smallestId - smallestId % EWAHCompressedBitmap.WORD_IN_BITS + EWAHCompressedBitmap.WORD_IN_BITS)) {
            // start and stop in same word
            int firstOne = smallestId - initialZeroWords * EWAHCompressedBitmap.WORD_IN_BITS;
            int numberOfOnes = largestId - smallestId + 1;
            long word = 0;
            for (int i = 0; i < numberOfOnes; i++) {
                word |= (1l << (firstOne + i));
            }
            mask.addWord(word);
        } else if (largestId > smallestId) {
            // start word, run of ones, stop word
            int onesInStartWord = EWAHCompressedBitmap.WORD_IN_BITS - smallestId % EWAHCompressedBitmap.WORD_IN_BITS;
            if (onesInStartWord == EWAHCompressedBitmap.WORD_IN_BITS) {
                onesInStartWord = 0;
            }
            if (onesInStartWord > 0) {
                long startWord = 0;
                for (int i = 0; i < onesInStartWord; i++) {
                    startWord |= (1l << (63 - i));
                }
                mask.addWord(startWord);
            }

            int middleOneWords = (largestId - smallestId - onesInStartWord) / EWAHCompressedBitmap.WORD_IN_BITS;
            if (middleOneWords > 0) {
                mask.addStreamOfEmptyWords(true, middleOneWords);
            }

            int bitsInStopWord = largestId - smallestId + 1 - onesInStartWord - middleOneWords * EWAHCompressedBitmap.WORD_IN_BITS;
            if (bitsInStopWord > 0) {
                long stopWord = 0;
                for (int i = 0; i < bitsInStopWord; i++) {
                    stopWord |= (1l << i);
                }
                mask.addWord(stopWord);
            }
        }

        return mask;
    }


    @Override
    public void copy(EWAHCompressedBitmap container, EWAHCompressedBitmap original) {
        original.orToContainer(EMPTY, container); // TODO. hmmmm. fix ME
    }

    @Override
    public MiruIntIterator intIterator(EWAHCompressedBitmap bitmap) {
        final IntIterator intIterator = bitmap.intIterator();
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
    public int lastSetBit(EWAHCompressedBitmap bitmap) {
        MiruIntIterator iterator = intIterator(bitmap);
        int last = -1;
        while (iterator.hasNext()) {
            last = iterator.next();
        }
        return last;
    }
}
