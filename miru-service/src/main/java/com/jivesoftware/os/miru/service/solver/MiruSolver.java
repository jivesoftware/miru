package com.jivesoftware.os.miru.service.solver;

import com.google.common.base.Optional;
import com.jivesoftware.os.miru.api.MiruPartitionCoord;
import com.jivesoftware.os.miru.api.activity.MiruPartitionId;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.plugin.solution.MiruSolutionLog;
import com.jivesoftware.os.miru.plugin.solution.MiruSolvable;
import java.util.Iterator;
import java.util.concurrent.Executor;

/**
 * Use the solver to solve the solvables, getting back a solved solution!
 */
public interface MiruSolver {

    <R> MiruSolved<R> solve(String requestName,
        String queryKey,
        MiruTenantId tenantId,
        MiruPartitionId partitionId,
        Iterator<MiruSolvable<R>> solvables,
        Optional<Long> suggestedTimeoutInMillis,
        Executor executor,
        MiruSolutionLog solutionLog)
        throws InterruptedException;

}
