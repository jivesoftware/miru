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
import com.jivesoftware.os.miru.service.index.MiruTimeIndex;
import java.io.DataInput;
import java.io.DataOutput;
import java.util.Collection;
import java.util.List;

/**
 *
 * @author jonathan
 */
public interface MiruBitmaps<BM> {

    BM create();

    BM[] createArrayOf(int size);

    boolean set(BM bitmap, int i);

    boolean isSet(BM bitmap, int i);

    boolean extend(BM bitmap, int i);

    void clear(BM bitmap);

    long cardinality(BM bitmap);

    long sizeInBytes(BM bitmap);

    long sizeInBits(BM bitmap);

    long serializedSizeInBytes(BM bitmap);

    BM deserialize(DataInput dataInput) throws Exception;

    void serialize(BM bitmap, DataOutput dataOutput) throws Exception;

    void or(BM container, Collection<BM> bitmaps);

    void and(BM container, Collection<BM> bitmaps);

    void andNot(BM container, BM original, List<BM> not);

    void copy(BM container, BM original);

    CardinalityAndLastSetBit andNotWithCardinalityAndLastSetBit(BM container, BM original, BM not);

    CardinalityAndLastSetBit andWithCardinalityAndLastSetBit(BM container, List<BM> ands);

    void orToSourceSize(BM container, BM source, BM mask);

    void andNotToSourceSize(BM container, BM source, BM mask);

    BM buildIndexMask(int largestIndex, Optional<BM> andNotMask);

    BM buildTimeRangeMask(MiruTimeIndex timeIndex, long smallestTimestamp, long largestTimestamp);

    MiruIntIterator intIterator(BM bitmap);

    static public class CardinalityAndLastSetBit {

        public long cardinality;
        public int lastSetBit;

        public CardinalityAndLastSetBit(long cardintality, int lastSetBit) {
            this.cardinality = cardintality;
            this.lastSetBit = lastSetBit;
        }
    }

}
