package com.jivesoftware.os.miru.reco.trending;

import org.apache.commons.math.stat.regression.SimpleRegression;

/**
 * @author jonathan
 */
public class SlopeTrendRank implements TrendRank<SimpleRegression> {

    @Override
    public double getRank(Long time, SimpleRegression r) {
        return r.getSlope();
    }

}
