package com.jivesoftware.os.miru.service.solver;

import com.google.common.base.Optional;
import com.jivesoftware.os.miru.plugin.solution.MiruSolutionLog;
import com.jivesoftware.os.miru.plugin.solution.MiruSolvable;
import java.util.Iterator;

/**
 * Use the solver to solve the solvables, getting back a solved solution!
 */
public interface MiruSolver {

    <R> MiruSolved<R> solve(String requestName,
        String queryKey,
        Iterator<MiruSolvable<R>> solvables,
        Optional<Long> suggestedTimeoutInMillis,
        MiruSolutionLog solutionLog)
        throws InterruptedException;

}
