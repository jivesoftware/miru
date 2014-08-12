package com.jivesoftware.os.miru.service.stream.factory;

import com.jivesoftware.os.miru.api.MiruPartitionCoord;
import java.util.concurrent.Callable;

/**
 *
 */
public class MiruSolvable<R> implements Callable<R> {

    private final MiruPartitionCoord coord;
    private final Callable<R> callable;
    private final Class<? extends ExecuteQuery<?, ?>> queryClass;

    public MiruSolvable(MiruPartitionCoord coord, Callable<R> callable, Class<? extends ExecuteQuery<?, ?>> queryClass) {
        this.coord = coord;
        this.callable = callable;
        this.queryClass = queryClass;
    }

    public MiruPartitionCoord getCoord() {
        return coord;
    }

    public Class<? extends ExecuteQuery<?, ?>> getQueryClass() {
        return queryClass;
    }

    @Override
    public R call() throws Exception {
        return callable.call();
    }
}
