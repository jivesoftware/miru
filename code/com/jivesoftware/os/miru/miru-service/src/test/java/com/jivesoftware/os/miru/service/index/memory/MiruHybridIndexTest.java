package com.jivesoftware.os.miru.service.index.memory;

import com.google.common.collect.Lists;
import gnu.trove.impl.Constants;
import gnu.trove.impl.hash.TPrimitiveHash;
import gnu.trove.map.hash.TLongObjectHashMap;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import org.testng.annotations.Test;

public class MiruHybridIndexTest {

    @Test(enabled = false)
    public void testKeyHash() {
        List<Strategy> strategies = Lists.newArrayList(
            new Strategy() {
                @Override
                public String name() {
                    return "reverse";
                }

                @Override
                public long getKey(int fieldId, int termId) {
                    return Long.reverse((((long) termId) << 32) | fieldId);
                }
            },
            new Strategy() {
                @Override
                public String name() {
                    return "random";
                }

                @Override
                public long getKey(int fieldId, int termId) {
                    return new Random(fieldId).nextLong() ^ new Random(termId).nextLong();
                }
            }
        );

        for (Strategy strategy : strategies) {
            runStrategy(strategy);
        }
    }

    private static void runStrategy(Strategy strategy) {
        Object object = new Object();
        int noEntryKey = -1;

        for (float loadFactor : new float[] { 0.1f, 0.3f, 0.5f, 0.7f, 0.9f }) {
            TLongObjectHashMap<Object> index = new TLongObjectHashMap<>(Constants.DEFAULT_CAPACITY, loadFactor, noEntryKey);

            int rounds = 5_000;
            for (int i = 0; i < rounds; i++) {
                for (int fieldId = 0; fieldId < 20; fieldId++) {
                    int termId = i * 20;
                    long fieldTermId = strategy.getKey(fieldId, termId);
                    index.put(fieldTermId, object);
                }
            }

            byte[] states = index._states;
            int runLength = 0;
            int[] histo = new int[10];
            for (long state : states) {
                if (state == TPrimitiveHash.FULL) {
                    runLength++;
                } else {
                    if (runLength > 0) {
                        //System.out.println(runLength);
                        histo[(int) (Math.log(runLength) / Math.log(2))]++;
                        runLength = 0;
                    }
                }
            }

            System.out.println("------------------------");
            System.out.println("lf: " + loadFactor + ", size: " + states.length);
            System.out.println(Arrays.toString(histo) + "\t" + strategy.name());
        }
    }

    private interface Strategy {

        String name();

        long getKey(int fieldId, int termId);
    }
}