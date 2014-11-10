package com.jivesoftware.os.miru.service.solver;

import com.google.common.base.Optional;
import com.jivesoftware.os.jive.utils.logger.MetricLogger;
import com.jivesoftware.os.jive.utils.logger.MetricLoggerFactory;
import com.jivesoftware.os.miru.api.MiruPartition;
import com.jivesoftware.os.miru.api.MiruPartitionCoord;
import com.jivesoftware.os.miru.plugin.solution.MiruPartitionResponse;
import com.jivesoftware.os.miru.plugin.solution.MiruSolution;
import com.jivesoftware.os.miru.plugin.solution.MiruSolutionLog;
import com.jivesoftware.os.miru.plugin.solution.MiruSolvable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/**
 * @author jonathan
 */
public class MiruLowestLatencySolver implements MiruSolver {

    private static final MetricLogger log = MetricLoggerFactory.getLogger();

    private final Executor executor;
    private final int initialSolvers;
    private final int maxNumberOfSolvers;
    private final long defaultAddAnotherSolverAfterNMillis;
    private final long failAfterNMillis;

    public MiruLowestLatencySolver(
            Executor executor,
            int initialSolvers,
            int maxNumberOfSolvers,
            long defaultAddAnotherSolverAfterNMillis,
            long failAfterNMillis) {
        this.executor = executor;
        this.initialSolvers = initialSolvers;
        this.maxNumberOfSolvers = maxNumberOfSolvers;
        this.defaultAddAnotherSolverAfterNMillis = defaultAddAnotherSolverAfterNMillis;
        this.failAfterNMillis = failAfterNMillis;
    }

    @Override
    public <R> MiruSolved<R> solve(Iterator<MiruSolvable<R>> solvables,
            Optional<Long> suggestedTimeoutInMillis,
            List<MiruPartition> orderedPartitions,
            MiruSolutionLog solutionLog)
            throws InterruptedException {

        long failAfterTime = System.currentTimeMillis() + failAfterNMillis;
        long addAnotherSolverAfterNMillis = suggestedTimeoutInMillis.or(defaultAddAnotherSolverAfterNMillis);

        CompletionService<MiruPartitionResponse<R>> completionService = new ExecutorCompletionService<>(executor);
        int solversAdded = 0;
        int solversFailed = 0;
        long startTime = System.currentTimeMillis();
        List<SolvableFuture<R>> futures = new ArrayList<>(initialSolvers);
        List<MiruPartitionCoord> triedPartitions = new ArrayList<>(initialSolvers);
        MiruSolved<R> solved = null;
        try {
            while (solvables.hasNext() && solversAdded < initialSolvers) {
                MiruSolvable<R> solvable = solvables.next();
                solutionLog.log("Initial solver index={} coord={}", solversAdded, solvable.getCoord());
                triedPartitions.add(solvable.getCoord());
                futures.add(new SolvableFuture<>(solvable, completionService.submit(solvable), System.currentTimeMillis()));
                solversAdded++;
            }
            while (solversFailed < maxNumberOfSolvers && System.currentTimeMillis() < failAfterTime) {
                boolean mayAddSolver = (solversAdded < maxNumberOfSolvers && solvables.hasNext());
                long timeout = Math.max(failAfterTime - System.currentTimeMillis(), 0);
                if (timeout == 0) {
                    break; // out of time
                }
                if (mayAddSolver) {
                    timeout = Math.min(timeout, addAnotherSolverAfterNMillis);
                }
                Future<MiruPartitionResponse<R>> future = completionService.poll(timeout, TimeUnit.MILLISECONDS);
                if (future != null) {
                    try {
                        MiruPartitionResponse<R> response = future.get();
                        if (response != null) {
                            // should be few enough of these that we prefer a linear lookup
                            for (SolvableFuture<R> f : futures) {
                                if (f.future == future) {
                                    solutionLog.log("Got a solution coord={}.", f.solvable.getCoord());
                                    long usedResultElapsed = System.currentTimeMillis() - f.startTime;
                                    long totalElapsed = System.currentTimeMillis() - startTime;
                                    solved = new MiruSolved<>(
                                            new MiruSolution(f.solvable.getCoord(),
                                                    usedResultElapsed,
                                                    totalElapsed,
                                                    orderedPartitions,
                                                    triedPartitions,
                                                    response.log),
                                            response.answer);
                                    break;
                                }
                            }
                            if (solved == null) {
                                log.error("Unmatched future");
                                solutionLog.log("Unmatched future.");
                            }
                            break;
                        }
                    } catch (ExecutionException e) {
                        log.debug("Solver failed to execute", e.getCause());
                        solutionLog.log("Solver failed to execute.");
                        solversFailed++;
                    }
                }
                if (mayAddSolver) {
                    MiruSolvable<R> solvable = solvables.next();
                    solutionLog.log("Added a solver coord={}", solvable.getCoord());
                    triedPartitions.add(solvable.getCoord());
                    futures.add(new SolvableFuture<>(solvable, completionService.submit(solvable), System.currentTimeMillis()));
                    solversAdded++;
                }
            }
        } finally {
            for (SolvableFuture<R> f : futures) {
                f.future.cancel(true);
            }
        }

        return solved;
    }

    private static class SolvableFuture<R> {

        private final MiruSolvable<R> solvable;
        private final Future<MiruPartitionResponse<R>> future;
        private final long startTime;

        private SolvableFuture(MiruSolvable<R> solvable, Future<MiruPartitionResponse<R>> future, long startTime) {
            this.solvable = solvable;
            this.future = future;
            this.startTime = startTime;
        }
    }
}
