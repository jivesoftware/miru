package com.jivesoftware.os.miru.service.partition;

import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;

public class RunningPercentileTest {

    @Test
    public void testAscendingInsertions() throws Exception {
        int windowSize = 100;
        int percentile = 95;
        RunningPercentile rp = new RunningPercentile(windowSize, percentile);

        int iterations = 10; // walk through the window multiple times
        for (int i = 0; i < iterations; i++) {
            for (long t = 1; t <= windowSize; t++) {
                rp.add(t);
            }
        }

        assertEquals(rp.get(), percentile);
    }

    @Test
    public void testDescendingInsertions() throws Exception {
        int windowSize = 100;
        int percentile = 95;
        RunningPercentile rp = new RunningPercentile(windowSize, percentile);

        int iterations = 10; // walk through the window multiple times
        for (int i = 0; i < iterations; i++) {
            for (long t = windowSize; t >= 1; t--) {
                rp.add(t);
            }
        }

        assertEquals(rp.get(), percentile);
    }

    @Test
    public void testAlternatingInsertions() throws Exception {
        int windowSize = 100;
        int percentile = 95;
        RunningPercentile rp = new RunningPercentile(windowSize, percentile);

        int iterations = 10; // walk through the window multiple times
        for (int i = 0; i < iterations; i++) {
            for (long t = 1; t <= windowSize / 2; t++) {
                rp.add(t);
                rp.add(windowSize + 1 - t);
            }
        }

        assertEquals(rp.get(), percentile);
    }
}
