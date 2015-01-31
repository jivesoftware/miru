package com.jivesoftware.os.miru.service.index.memory;

import gnu.trove.impl.Constants;
import gnu.trove.impl.hash.TPrimitiveHash;
import gnu.trove.map.hash.TLongIntHashMap;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import org.testng.annotations.Test;

public class MiruInMemoryTimeIndexTest {

    @Test(enabled = false)
    public void testKeyHash() {

        final long start = System.currentTimeMillis();

        List<Strategy> strategies = Arrays.asList(
            new Strategy() {
                @Override
                public String name() {
                    return "timestamp";
                }

                @Override
                public long getKey(long timestamp) {
                    return timestamp;
                }
            },
            new Strategy() {
                @Override
                public String name() {
                    return "reverse";
                }

                @Override
                public long getKey(long timestamp) {
                    return Long.reverse(timestamp);
                }
            },
            new Strategy() {
                @Override
                public String name() {
                    return "random";
                }

                @Override
                public long getKey(long timestamp) {
                    return new Random(timestamp).nextLong();
                }
            }
        );

        for (Strategy strategy : strategies) {
            runStrategy(strategy, start);
        }
    }

    private static void runStrategy(Strategy strategy, long start) {
        int noEntryKey = -1;
        int noEntryValue = -1;

        for (float loadFactor : new float[] { 0.1f, 0.3f, 0.5f, 0.7f, 0.9f }) {
            TLongIntHashMap index = new TLongIntHashMap(Constants.DEFAULT_CAPACITY, loadFactor, noEntryKey, noEntryValue);

            Random r = new Random(12345);
            int rounds = 5_000_000;
            long timestamp = start;
            for (int i = 0; i < rounds; i++) {
                timestamp += r.nextInt(10_000);
                index.put(strategy.getKey(timestamp), 1);
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

        long getKey(long timestamp);
    }
}
