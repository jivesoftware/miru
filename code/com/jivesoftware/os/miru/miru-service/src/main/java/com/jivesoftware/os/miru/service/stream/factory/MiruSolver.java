package com.jivesoftware.os.miru.service.stream.factory;

import com.google.common.base.Optional;
import com.jivesoftware.os.miru.query.MiruSolvable;
import java.util.Iterator;

/**
 *
 */
public interface MiruSolver {

    <R> MiruSolution<R> solve(Iterator<MiruSolvable<R>> solvables, Optional<Long> suggestedTimeoutInMillis) throws InterruptedException;

}
