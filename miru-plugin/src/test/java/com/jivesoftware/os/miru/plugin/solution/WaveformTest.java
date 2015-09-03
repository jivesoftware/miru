package com.jivesoftware.os.miru.plugin.solution;

import java.util.Arrays;
import java.util.Random;
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 *
 */
public class WaveformTest {

    @Test
    public void testRandomWithPrecision() {
        Random random = new Random();
        final int numRuns = 10;
        final int numBuckets = 4;
        for (int run = 0; run < numRuns; run++) {
            System.out.println("run " + run);
            for (int offset = 0; offset < numBuckets; offset++) {
                int count = numBuckets - offset * 2;
                for (int precision = 0; precision <= 8; precision++) {
                    long[] rawWaveform = new long[numBuckets];
                    for (int i = offset; i < offset + count; i++) {
                        long v = random.nextInt(256) - 128;
                        for (int p = 1; p < precision; p++) {
                            v *= 256;
                        }
                        rawWaveform[i] = v;
                    }
                    assertWaveform(rawWaveform);
                }
            }
        }
    }

    @Test
    public void testEdgeCases() {
        assertWaveform(new long[] { 0, 0, 0, 0 });
        assertWaveform(new long[] { -1, -1, -1, -1 });
        assertWaveform(new long[] { 0, -128, 127, 0 });
        assertWaveform(new long[] { 0, -129, 128, 0 });
        assertWaveform(new long[] { Long.MIN_VALUE, Long.MAX_VALUE, 0, 0 });
    }

    private void assertWaveform(long[] expected) {
        Waveform waveform = new Waveform(expected);
        long[] actual = new long[expected.length];
        waveform.mergeWaveform(actual);
        Assert.assertTrue(Arrays.equals(expected, actual), Arrays.toString(expected) + " != " + Arrays.toString(actual));
    }

}