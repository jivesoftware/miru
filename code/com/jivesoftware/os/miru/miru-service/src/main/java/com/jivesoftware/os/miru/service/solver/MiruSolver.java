package com.jivesoftware.os.miru.service.solver;

import com.google.common.base.Optional;
import com.jivesoftware.os.miru.api.MiruPartition;
import com.jivesoftware.os.miru.query.solution.MiruSolvable;
import java.util.Iterator;
import java.util.List;

/**
 * Use the solver to solve the solvables, getting back a solved solution!
 */
public interface MiruSolver {

    <R> MiruSolved<R> solve(Iterator<MiruSolvable<R>> solvables, Optional<Long> suggestedTimeoutInMillis, List<MiruPartition> considered)
            throws InterruptedException;

}
