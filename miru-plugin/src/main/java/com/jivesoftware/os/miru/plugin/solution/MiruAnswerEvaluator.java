package com.jivesoftware.os.miru.plugin.solution;

/**
 *
 */
public interface MiruAnswerEvaluator<A> {

    /**
     * Determines whether a solution is complete given a result.
     * @param result the result
     * @param solutionLog the solution log
     * @return true if the solution is complete, otherwise false
     */
    boolean isDone(A result, MiruSolutionLog solutionLog);

    /**
     * Determines how we handle unsolvable replicas.
     *
     * @return true if solving should continue beyond unsolvable replicas, or false if an unsolvable replica should halt the solver
     */
    boolean stopOnUnsolvablePartition();
}
