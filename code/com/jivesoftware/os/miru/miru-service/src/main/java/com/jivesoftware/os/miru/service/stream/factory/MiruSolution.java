package com.jivesoftware.os.miru.service.stream.factory;

import com.jivesoftware.os.miru.api.MiruPartitionCoord;

/**
 *
 */
public class MiruSolution<R> {

    private final R result;
    private final MiruPartitionCoord coord;
    private final String queryClass;
    private final long elapsed;

    public MiruSolution(R result, MiruPartitionCoord coord, String queryClass, long elapsed) {
        this.result = result;
        this.coord = coord;
        this.queryClass = queryClass;
        this.elapsed = elapsed;
    }

    public R getResult() {
        return result;
    }

    public MiruPartitionCoord getCoord() {
        return coord;
    }

    public String getQueryClass() {
        return queryClass;
    }

    public long getElapsed() {
        return elapsed;
    }
}
