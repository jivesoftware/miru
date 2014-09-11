package com.jivesoftware.os.miru.service.stream.factory;

import com.jivesoftware.os.miru.query.MiruSolution;

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
