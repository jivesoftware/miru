package com.jivesoftware.os.miru.reco.trending;

import org.apache.commons.math.stat.regression.SimpleRegression;

/**
 *
 */
public class WaveformRegression {

    public static SimpleRegression getRegression(long[] smooth) {
        double[] raw = new double[smooth.length];
        for (int i = 0; i < smooth.length; i++) {
            raw[i] = smooth[i];
        }
        return getRegression(raw);
    }

    public static SimpleRegression getRegression(double[] smooth) {
        SimpleRegression r = new SimpleRegression();
        int l = smooth.length;
        for (int i = 0; i < l; i++) {
            double s = i / (l - 1);
            r.addData(s, smooth[i]);
        }
        return r;
    }

    private WaveformRegression() {
    }
}
