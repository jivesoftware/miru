package com.jivesoftware.os.miru.service.stream.factory;

import com.google.common.base.Optional;
import java.util.Iterator;

/**
 *
 */
public interface MiruSolver {

    <R> MiruSolution<R> solve(Iterator<MiruSolvable<R>> solvables, Optional<Long> suggestedTimeoutInMillis) throws InterruptedException;

}
