package com.jivesoftware.os.miru.reco.trending;

/**
 * @author jonathan
 */
public interface TrendRank<T> {
    public double getRank(Long time, T trend);
}
