package com.jivesoftware.os.miru.plugin.solution;

import com.jivesoftware.os.miru.api.MiruPartitionCoord;
import java.util.concurrent.Callable;

/**
 *
 */
public class MiruSolvable<R> implements Callable<MiruPartitionResponse<R>> {

    private final MiruPartitionCoord coord;
    private final Callable<MiruPartitionResponse<R>> callable;
    private final boolean local;

    public MiruSolvable(MiruPartitionCoord coord, Callable<MiruPartitionResponse<R>> callable, boolean local) {
        this.coord = coord;
        this.callable = callable;
        this.local = local;
    }

    public MiruPartitionCoord getCoord() {
        return coord;
    }

    public boolean isLocal() {
        return local;
    }

    @Override
    public MiruPartitionResponse<R> call() throws Exception {
        return callable.call();
    }
}
