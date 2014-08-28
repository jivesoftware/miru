package com.jivesoftware.os.miru.query;

import com.jivesoftware.os.miru.api.MiruPartitionCoord;
import java.util.concurrent.Callable;

/**
 *
 */
public class MiruSolvable<R> implements Callable<R> {

    private final MiruPartitionCoord coord;
    private final Callable<R> callable;

    public MiruSolvable(MiruPartitionCoord coord, Callable<R> callable) {
        this.coord = coord;
        this.callable = callable;
    }

    public MiruPartitionCoord getCoord() {
        return coord;
    }

    @Override
    public R call() throws Exception {
        return callable.call();
    }
}
