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
    private final MiruSolutionLog solutionLog;

    public MiruSolvable(MiruPartitionCoord coord, Callable<MiruPartitionResponse<R>> callable, boolean local, MiruSolutionLog solutionLog) {
        this.coord = coord;
        this.callable = callable;
        this.local = local;
        this.solutionLog = solutionLog;
    }

    public MiruSolutionLog getSolutionLog() {
        return solutionLog;
    }

    public MiruPartitionCoord getCoord() {
        return coord;
    }

    public boolean isLocal() {
        return local;
    }

    @Override
    public MiruPartitionResponse<R> call() throws Exception {
        if (solutionLog != null) {
            solutionLog.log(MiruSolutionLogLevel.INFO, "Executing local={} coord={}", local, coord);
        }
        return callable.call();
    }
}
