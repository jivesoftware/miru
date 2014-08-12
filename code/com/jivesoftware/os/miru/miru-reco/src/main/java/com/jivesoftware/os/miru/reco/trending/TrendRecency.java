package com.jivesoftware.os.miru.reco.trending;

/**
 * @author jonathan
 */
public interface TrendRecency<T> {
    public double getRecency(Long time, T trend);
}
