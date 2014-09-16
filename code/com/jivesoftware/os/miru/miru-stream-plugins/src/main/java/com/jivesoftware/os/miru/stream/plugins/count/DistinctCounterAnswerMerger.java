package com.jivesoftware.os.miru.stream.plugins.count;

import com.google.common.base.Optional;
import com.jivesoftware.os.miru.query.solution.MiruAnswerMerger;
import com.jivesoftware.os.miru.query.solution.MiruSolutionLog;

/**
 * @author jonathan
 */
public class DistinctCounterAnswerMerger implements MiruAnswerMerger<DistinctCountAnswer> {

    @Override
    public DistinctCountAnswer merge(Optional<DistinctCountAnswer> last, DistinctCountAnswer current, MiruSolutionLog solutionLog) {
        // TODO the merging is actually done by the filtering process, consider revisiting this
        return current;
    }

    @Override
    public DistinctCountAnswer done(Optional<DistinctCountAnswer> last, DistinctCountAnswer alternative, MiruSolutionLog solutionLog) {
        return last.or(alternative);
    }
}
