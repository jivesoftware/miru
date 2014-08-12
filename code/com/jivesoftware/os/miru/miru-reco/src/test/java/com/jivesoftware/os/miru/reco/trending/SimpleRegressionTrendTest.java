package com.jivesoftware.os.miru.reco.trending;

import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class SimpleRegressionTrendTest {

    @Test
    public void testMerge() throws Exception {
        long durationPerBucket = SimpleRegressionTrend.durationPerBucket;
        int numberOfBuckets = SimpleRegressionTrend.numberOfBuckets;

        SimpleRegressionTrend trend1 = new SimpleRegressionTrend();
        SimpleRegressionTrend trend2 = new SimpleRegressionTrend();

        long currentT = trend1.getCurrentT();

        for (int i = 0; i < numberOfBuckets; i++) {
            trend1.add(currentT - i * durationPerBucket, (double) (i + 1));
            trend2.add(currentT - i * durationPerBucket, (double) (i + 1));
        }

        SimpleRegressionTrend merged = new SimpleRegressionTrend();
        merged.merge(trend1);
        merged.merge(trend2);

        merged.add(currentT, 0d);
        double[] rawSignal = merged.getRawSignal();
        for (int i = 0; i < numberOfBuckets; i++) {
            assertEquals(rawSignal[i], 2 * (double) (numberOfBuckets - i));
        }
    }

    @Test
    public void testRank() throws Exception {
        long durationPerBucket = SimpleRegressionTrend.durationPerBucket;
        int numberOfBuckets = SimpleRegressionTrend.numberOfBuckets;

        SimpleRegressionTrend flat = new SimpleRegressionTrend();
        SimpleRegressionTrend line = new SimpleRegressionTrend();
        SimpleRegressionTrend quad = new SimpleRegressionTrend();

        long currentT = flat.getCurrentT();

        for (int i = 0; i < numberOfBuckets; i++) {
            long time = currentT - (numberOfBuckets - i - 1) * durationPerBucket;
            flat.add(time, 1d);
            line.add(time, 1d * i);
            quad.add(time, (double) (i * i));
        }

        assertTrue(flat.getRank(currentT) < line.getRank(currentT));
        assertTrue(line.getRank(currentT) < quad.getRank(currentT));
    }
}