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
package com.jivesoftware.os.miru.bitmaps.roaring6;

import com.google.common.base.Optional;
import com.jivesoftware.os.filer.io.ByteBufferDataInput;
import com.jivesoftware.os.filer.io.Filer;
import com.jivesoftware.os.filer.io.FilerDataInput;
import com.jivesoftware.os.filer.io.api.StackBuffer;
import com.jivesoftware.os.filer.io.chunk.ChunkFiler;
import com.jivesoftware.os.miru.plugin.bitmap.MiruBitmaps;
import com.jivesoftware.os.miru.plugin.bitmap.MiruIntIterator;
import com.jivesoftware.os.miru.plugin.index.BitmapAndLastId;
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
import java.util.List;
import java.util.concurrent.ExecutorService;
import org.roaringbitmap.FastAggregation;
import org.roaringbitmap.IntIterator;
import org.roaringbitmap.RoaringBitmap;
import org.roaringbitmap.RoaringInspection;

/**
 * @author jonathan
 */
public class MiruBitmapsRoaring implements MiruBitmaps<RoaringBitmap, RoaringBitmap> {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    private boolean addInPlace(RoaringBitmap bitmap, int... indexes) {
        if (indexes.length == 1) {
            bitmap.add(indexes[0]);
        } else if (indexes.length > 1) {
            int rangeStart = 0;
            for (int rangeEnd = 1; rangeEnd < indexes.length; rangeEnd++) {
                if (indexes[rangeEnd - 1] + 1 != indexes[rangeEnd]) {
                    if (rangeStart == rangeEnd - 1) {
                        bitmap.add(indexes[rangeStart]);
                    } else {
                        bitmap.add(indexes[rangeStart], indexes[rangeEnd - 1] + 1);
                    }
                    rangeStart = rangeEnd;
                }
            }
            if (rangeStart == indexes.length - 1) {
                bitmap.add(indexes[rangeStart]);
            } else {
                bitmap.add(indexes[rangeStart], indexes[indexes.length - 1] + 1);
            }
        }
        return true;
    }

    @Override
    public RoaringBitmap set(RoaringBitmap bitmap, int... indexes) {
        RoaringBitmap container = copy(bitmap);
        addInPlace(container, indexes);
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
        return new RoaringBitmap[size];
    }

    @Override
    public RoaringBitmap[] createImmutableArrayOf(int size) {
        return new RoaringBitmap[size];
    }

    @Override
    public RoaringBitmap[][] createMultiArrayOf(int size1, int size2) {
        return new RoaringBitmap[size1][size2];
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
        multiTermTxIndex.txIndex((index, lastId, bitmap, filer, offset, stackBuffer1) -> {
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

    public void inPlaceAndNotParallel(RoaringBitmap original, RoaringBitmap not, ExecutorService executorService) {
        //RoaringAggregation.andNotParallel(original, not, executorService);
    }

    @Override
    public void inPlaceAndNot(RoaringBitmap original, MiruInvertedIndex<RoaringBitmap, RoaringBitmap> not, StackBuffer stackBuffer) throws Exception {
        BitmapAndLastId<RoaringBitmap> index = new BitmapAndLastId<>();
        not.getIndex(index, stackBuffer);
        if (index.isSet()) {
            original.andNot(index.getBitmap());
        }
    }

    @Override
    public RoaringBitmap andNotMultiTx(RoaringBitmap original,
        MiruMultiTxIndex<RoaringBitmap> multiTermTxIndex,
        long[] counts,
        Optional<RoaringBitmap> counter,
        StackBuffer stackBuffer) throws Exception {

        RoaringBitmap container = copy(original);
        inPlaceAndNotMultiTx(container, multiTermTxIndex, counts, counter, stackBuffer);
        return container;
    }

    @Override
    public void inPlaceAndNotMultiTx(RoaringBitmap original,
        MiruMultiTxIndex<RoaringBitmap> multiTermTxIndex,
        long[] counts,
        Optional<RoaringBitmap> counter,
        StackBuffer stackBuffer) throws Exception {
        long[] originalCardinality = (counts == null) ? null : new long[] { cardinality(counter.or(original)) };
        multiTermTxIndex.txIndex((index, lastId, bitmap, filer, offset, stackBuffer1) -> {
            if (bitmap != null) {
                original.andNot(bitmap);
                if (counter.isPresent()) {
                    counter.get().andNot(bitmap);
                }
            } else if (filer != null) {
                RoaringBitmap bitmapFromFiler = bitmapFromFiler(filer, offset, stackBuffer1);
                original.andNot(bitmapFromFiler);
                if (counter.isPresent()) {
                    counter.get().andNot(bitmapFromFiler);
                }
            }
            if (counts != null) {
                long nextCardinality = cardinality(counter.or(original));
                counts[index] = originalCardinality[0] - nextCardinality;
                originalCardinality[0] = nextCardinality;
            }
        }, stackBuffer);
    }

    @Override
    public void multiTx(MiruMultiTxIndex<RoaringBitmap> multiTermTxIndex,
        IndexAlignedBitmapStream<RoaringBitmap> stream,
        StackBuffer stackBuffer) throws Exception {

        multiTermTxIndex.txIndex((index, lastId, bitmap, filer, offset, stackBuffer1) -> {
            if (bitmap != null) {
                stream.stream(index, lastId, copy(bitmap));
            } else if (filer != null) {
                stream.stream(index, lastId, bitmapFromFiler(filer, offset, stackBuffer1));
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

        RoaringBitmap container = original.txIndex((bitmap, filer, offset, stackBuffer1) -> {
            if (bitmap != null) {
                return bitmap;
            } else if (filer != null) {
                return bitmapFromFiler(filer, offset, stackBuffer1);
            } else {
                return new RoaringBitmap();
            }
        }, stackBuffer);

        if (container.isEmpty() || not.isEmpty()) {
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
    public RoaringBitmap buildIndexMask(int largestIndex,
        MiruInvertedIndex<RoaringBitmap, RoaringBitmap> removalIndex,
        BitmapAndLastId<RoaringBitmap> container,
        StackBuffer stackBuffer) throws Exception {

        RoaringBitmap mask = new RoaringBitmap();
        if (largestIndex < 0) {
            return mask;
        }

        mask.flip(0, largestIndex + 1);
        if (removalIndex != null) {
            if (container == null) {
                container = new BitmapAndLastId<>();
            }
            removalIndex.getIndex(container, stackBuffer);
            if (container.isSet()) {
                mask.andNot(container.getBitmap());
            }
        }
        return mask;
    }

    @Override
    public RoaringBitmap buildIndexMask(int smallestIndex,
        int largestIndex,
        MiruInvertedIndex<RoaringBitmap, RoaringBitmap> removalIndex,
        BitmapAndLastId<RoaringBitmap> container,
        StackBuffer stackBuffer) throws Exception {

        RoaringBitmap mask = new RoaringBitmap();
        if (largestIndex < 0 || smallestIndex > largestIndex) {
            return mask;
        }

        mask.flip(smallestIndex, largestIndex + 1);
        if (removalIndex != null) {
            if (container == null) {
                container = new BitmapAndLastId<>();
            }
            removalIndex.getIndex(container, stackBuffer);
            if (container.isSet()) {
                mask.andNot(container.getBitmap());
            }
        }
        return mask;
    }

    @Override
    public RoaringBitmap buildTimeRangeMask(MiruTimeIndex timeIndex,
        long smallestTimestamp,
        long largestTimestamp,
        StackBuffer stackBuffer) throws Exception {

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
    public int firstSetBit(RoaringBitmap bitmap) {
        return bitmap.isEmpty() ? -1 : bitmap.first();
    }

    @Override
    public int lastSetBit(RoaringBitmap bitmap) {
        return bitmap.isEmpty() ? -1 : bitmap.last();
    }

    @Override
    public int firstIntersectingBit(RoaringBitmap bitmap, RoaringBitmap other) {
        return RoaringInspection.firstIntersectingBit(bitmap, other);
    }

    @Override
    public boolean intersects(RoaringBitmap bitmap, RoaringBitmap other) {
        return RoaringBitmap.intersects(bitmap, other);
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

    @Override
    public RoaringBitmap[] split(RoaringBitmap bitmap) {
        return RoaringInspection.split(bitmap);
    }

    @Override
    public RoaringBitmap[] extract(RoaringBitmap bitmap, int[] keys) {
        return RoaringInspection.extract(bitmap, keys);
    }

    @Override
    public int key(int position) {
        return RoaringInspection.key(position);
    }

    @Override
    public int[] keys(RoaringBitmap mask) {
        return RoaringInspection.keys(mask);
    }

    @Override
    public long[] serializeAtomizedSizeInBytes(RoaringBitmap index, int[] keys) {
        return RoaringInspection.serializeSizeInBytes(index, keys);
    }

    @Override
    public void serializeAtomized(RoaringBitmap index, int[] keys, DataOutput[] dataOutputs) throws IOException {
        RoaringInspection.userializeAtomized(index, keys, dataOutputs);
    }

    @Override
    public boolean deserializeAtomized(BitmapAndLastId<RoaringBitmap> container, StreamAtoms streamAtoms) throws IOException {
        return RoaringInspection.udeserialize(container, streamAtoms);
    }

    @Override
    public int lastIdAtomized(DataInput dataInput, int key) throws IOException {
        return RoaringInspection.lastSetBit(key, dataInput);
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
}
