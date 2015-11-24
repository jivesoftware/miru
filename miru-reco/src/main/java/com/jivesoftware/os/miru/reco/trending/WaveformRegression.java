package com.jivesoftware.os.miru.reco.trending;

import org.apache.commons.math.stat.regression.SimpleRegression;

/**
 *
 */
public class WaveformRegression {

    private final SimpleRegression simpleRegression = new SimpleRegression();

    public void add(long[] samples, int start, int length) {
        int l = samples.length;
        for (int i = 0; i < l; i++) {
            double s = (double) i / (double) (l - 1);
            simpleRegression.addData(s, samples[i]);
        }
    }

    public void clear() {
        simpleRegression.clear();
    }

    public double slope() {
        return simpleRegression.getSlope();
    }
}
