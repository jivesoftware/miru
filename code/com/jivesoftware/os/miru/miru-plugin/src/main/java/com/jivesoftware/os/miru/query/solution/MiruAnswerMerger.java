/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package com.jivesoftware.os.miru.query.solution;

import com.google.common.base.Optional;

/**
 *
 * @author jonathan
 */
public interface MiruAnswerMerger<A> {

    A merge(Optional<A> last, A current, MiruSolutionLog solutionLog);

    A done(Optional<A> last, A alternative, MiruSolutionLog solutionLog);

}
