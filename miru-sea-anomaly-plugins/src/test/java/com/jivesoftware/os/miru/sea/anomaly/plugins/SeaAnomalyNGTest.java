package com.jivesoftware.os.miru.sea.anomaly.plugins;

import com.jivesoftware.os.miru.bitmaps.roaring5.MiruBitmapsRoaring;
import java.util.Arrays;
import org.roaringbitmap.RoaringBitmap;
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 *
 * @author jonathan.colt
 */
public class SeaAnomalyNGTest {

    @Test
    public void testMetricingSum() throws Exception {

        long[] waveform = {1, 2, 3, 4, 5, 6, 7, 8, 9, 0, 0, 0, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
        waveformAsBitmaps(waveform);

        int[] indexes = new int[]{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23};

        long[] result = SeaAnomaly.sum(indexes, 64, Arrays.asList(waveformAsBitmaps(waveform)), new MiruBitmapsRoaring());

        System.out.println(Arrays.toString(result) + " vs " + Arrays.toString(waveform));
        Assert.assertTrue(Arrays.equals(waveform, result));
    }

    @Test
    public void testMetricingAvg() throws Exception {

        long[] waveform = {1, 1, 1, 2, 2, 2, 4, 8, 10, 0, 0, 0, 2, 0, 4};
        waveformAsBitmaps(waveform);

        int[] indexes = new int[]{0, 3, 6, 9, 12, 15};
        long[] expectedSum = new long[]{3, 6, 22, 0, 6};
        long[] expectedAvg = new long[]{1, 2, 7, 0, 2};

        MiruBitmapsRoaring miruBitmapsRoaring = new MiruBitmapsRoaring();

        long[] result = SeaAnomaly.sum(indexes, 64, Arrays.asList(waveformAsBitmaps(waveform)), miruBitmapsRoaring);

        RoaringBitmap rawAnswer = miruBitmapsRoaring.create();
        for (int i = 0; i < waveform.length; i++) {
            rawAnswer.add(i);
        }

        SeaAnomalyAnswer.Waveform metricingSum = SeaAnomaly.metricingSum(miruBitmapsRoaring, rawAnswer, Arrays.asList(waveformAsBitmaps(waveform)), indexes, 64);

        System.out.println("metricingSum expected=" + Arrays.toString(expectedSum) + " was=" + metricingSum);
        Assert.assertTrue(Arrays.equals(expectedSum, metricingSum.waveform), Arrays.toString(expectedSum) + " vs " + Arrays.toString(metricingSum.waveform));

        SeaAnomalyAnswer.Waveform metricingAvg = SeaAnomaly.metricingAvg(miruBitmapsRoaring, rawAnswer, Arrays.asList(waveformAsBitmaps(waveform)), indexes, 64);

        System.out.println("metricingAvg expected=" + Arrays.toString(expectedAvg) + " was=" + metricingAvg);
        Assert.assertTrue(Arrays.equals(expectedAvg, metricingAvg.waveform), Arrays.toString(expectedAvg) + " vs " + Arrays.toString(metricingAvg.waveform));

    }

    static RoaringBitmap[] waveformAsBitmaps(long... waveform) {
        MiruBitmapsRoaring bitmaps = new MiruBitmapsRoaring();
        RoaringBitmap[] bits = new RoaringBitmap[64 + 1];
        for (int i = 0; i < bits.length; i++) {
            bits[i] = bitmaps.create();
        }

        for (int i = 0; i < waveform.length; i++) {
            for (int j = 0; j < 64; j++) {
                if (((waveform[i] >>> j) & 1) != 0) {
                    bits[j].add(i);
                }
            }
            bits[64].add(i);
        }
        return bits;
    }

}
