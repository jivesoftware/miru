package com.jivesoftware.os.miru.reco.trending;

import org.apache.commons.math.stat.regression.SimpleRegression;

/**
 *
 */
public class WaveformRegression {

    public static SimpleRegression getRegression(long[] samples, int start, int length) {
        double[] raw = new double[length];
        for (int i = start; i < length; i++) {
            raw[i] = samples[i];
        }
        return getRegression(raw);
    }

    public static SimpleRegression getRegression(double[] samples) {
        SimpleRegression r = new SimpleRegression();
        int l = samples.length;
        for (int i = 0; i < l; i++) {
            double s = i / (l - 1);
            r.addData(s, samples[i]);
        }
        return r;
    }

    private WaveformRegression() {
    }
}
