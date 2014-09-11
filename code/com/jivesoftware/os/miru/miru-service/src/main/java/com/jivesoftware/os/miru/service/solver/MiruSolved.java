package com.jivesoftware.os.miru.service.solver;

import com.jivesoftware.os.miru.query.solution.MiruSolution;

/**
 *
 */
public class MiruSolved<A> {

    public final MiruSolution solution; // show your work!
    public final A answer; // circle the answer!

    public MiruSolved(MiruSolution solution, A answer) {
        this.solution = solution;
        this.answer = answer;
    }
}
