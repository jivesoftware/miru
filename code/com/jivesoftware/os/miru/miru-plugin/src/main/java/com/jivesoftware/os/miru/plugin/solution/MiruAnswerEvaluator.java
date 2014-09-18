package com.jivesoftware.os.miru.plugin.solution;

/**
 *
 */
public interface MiruAnswerEvaluator<A> {

    boolean isDone(A result, MiruSolutionLog solutionLog);
}
