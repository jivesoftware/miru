package com.jivesoftware.os.miru.reco.trending;

/**
 * @author jonathan
 */
public class LinearTimeDecayTrendRecency implements TrendRecency<SimpleRegressionTrend> {

    @Override
    public double getRecency(Long currentTime, SimpleRegressionTrend trend) {
        long elapse = currentTime - trend.getMostRecentTimestamp();
        if (elapse > trend.getDuration()) {
            return 0d; // no data
        }
        double normailzedTimeRank = (trend.getDuration() - elapse) / trend.getDuration();
        return normailzedTimeRank;
    }

}
