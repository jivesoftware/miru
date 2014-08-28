/*
 * Copyright 2014 jivesoftware.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law orToSourceSize agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express orToSourceSize implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.jivesoftware.os.miru.query;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ListMultimap;
import com.google.common.hash.HashFunction;
import com.jivesoftware.os.miru.api.base.MiruTermId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 *
 * @author jonathan
 */
public class BloomIndex<BM> {

    static public interface HasValue {

        byte[] getValue();
    }

    static public interface MightContain<V> {

        void mightContain(V value);
    }

    private final MiruBitmaps<BM> bitmaps;
    private final HashFunction hashFunction;
    private final int numBits;
    private final int numHashFunctions;

    public BloomIndex(MiruBitmaps<BM> bitmaps, HashFunction hashFunction, int expectedInsertions, float falsePositiveProbability) {
        this.bitmaps = bitmaps;
        this.hashFunction = hashFunction;

        long disiredBits = optimalNumOfBits(expectedInsertions, falsePositiveProbability);
        if (disiredBits > Integer.MAX_VALUE) {
            throw new IllegalArgumentException("expectedInsertions=" + expectedInsertions + " falsePositiveProbability=" + falsePositiveProbability
                    + " exceeds the capacity of an ewah.");
        }
        this.numBits = (int) disiredBits;
        this.numHashFunctions = optimalNumOfHashFunctions(expectedInsertions, this.numBits);
    }

    static long optimalNumOfBits(long n, double p) {
        if (p == 0) {
            p = Double.MIN_VALUE;
        }
        return (long) (-n * Math.log(p) / (Math.log(2) * Math.log(2)));
    }

    static int optimalNumOfHashFunctions(long n, long m) {
        return Math.max(1, (int) Math.round(m / n * Math.log(2)));
    }

    public void put(MiruInvertedIndex<BM> bloomIndex, MiruTermId[] keys) throws Exception {

        int[] bitIndexes = new int[keys.length * numHashFunctions];
        for (int i = 0; i < keys.length; i++) {
            MiruTermId key = keys[i];
            createBitIndexesForValue(key.getBytes(), numHashFunctions, bitIndexes, i * numHashFunctions);
        }
        bloomIndex.setIntermediate(bitIndexes);
    }

    public <V extends HasValue> List<Mights<V>> wantBits(List<V> keys) {
        ListMultimap<Integer, Might<V>> valueBitIndexes = ArrayListMultimap.create();
        for (V key : keys) {
            Might<V> might = new Might<>(key, numHashFunctions);
            int[] bitIndexes = new int[numHashFunctions];
            createBitIndexesForValue(key.getValue(), numHashFunctions, bitIndexes, 0);
            Arrays.sort(bitIndexes);
            for (Integer bitIndex : bitIndexes) {
                valueBitIndexes.put(bitIndex, might);
            }
        }

        List<Mights<V>> mights = new ArrayList<>();
        for (Integer key : valueBitIndexes.keySet()) {
            mights.add(new Mights<>(key, valueBitIndexes.get(key)));
        }
        Collections.sort(mights);
        return mights;
    }

    public <V extends HasValue> void mightContain(MiruInvertedIndex<BM> bloomIndex, List<Mights<V>> mights, MightContain<V> contains) throws Exception {

        BM bloomBitmap = bloomIndex.getIndex();
        MiruIntIterator setBits = bitmaps.intIterator(bloomBitmap);
        if (setBits.hasNext()) {
            int cursor = setBits.next();
            for (Mights<V> e : mights) {
                int bitIndex = e.bitIndex;
                while (true) {
                    if (cursor == bitIndex) {
                        for (Might<V> might : e.mights) {
                            if (might.mightContain()) {
                                contains.mightContain(might.getKey());
                            }
                        }
                    } else if (cursor > bitIndex) {
                        break;
                    }
                    if (setBits.hasNext()) {
                        cursor = setBits.next();
                    } else {
                        return;
                    }
                }
            }
        }

    }

    public static final class Mights<K extends HasValue> implements Comparable<Mights<K>> {

        final int bitIndex;
        final List<Might<K>> mights;

        public Mights(int bitIndex, List<Might<K>> mights) {
            this.bitIndex = bitIndex;
            this.mights = mights;
        }

        @Override
        public int compareTo(Mights<K> o) {
            return Integer.compare(bitIndex, o.bitIndex);
        }

        public void reset() {
            for (Might<K> might : mights) {
                might.reset();
            }
        }

        @Override
        public String toString() {
            return "Mights{" + "bitIndex=" + bitIndex + ", mights=" + mights + '}';
        }

    }

    public static class Might<K extends HasValue> {

        private final K key;
        private final int numBits;
        private int contains = 0;

        Might(K key, int numBits) {
            this.key = key;
            this.numBits = numBits;
        }

        public K getKey() {
            return key;
        }

        public boolean mightContain() {
            contains++;
            return contains == numBits;
        }

        private void reset() {
            contains = 0;
        }

        @Override
        public String toString() {
            return "Might{" + "key=" + key + ", numBits=" + numBits + ", contains=" + contains + '}';
        }

    }

    private void createBitIndexesForValue(byte[] value, int numHashFunctions, int[] bitIndexes, int offset) {
        long hash64 = hashFunction.hashBytes(value).asLong();
        int hash1 = (int) hash64;
        int hash2 = (int) (hash64 >>> 32);
        for (int i = 1; i <= numHashFunctions; i++) {
            int nextHash = hash1 + i * hash2;
            if (nextHash < 0) {
                nextHash = ~nextHash;
            }
            bitIndexes[offset + i - 1] = nextHash % numBits;
        }
    }

}
