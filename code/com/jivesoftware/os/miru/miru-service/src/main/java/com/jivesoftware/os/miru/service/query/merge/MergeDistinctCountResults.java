package com.jivesoftware.os.miru.service.query.merge;

import com.google.common.base.Optional;
import com.jivesoftware.os.miru.api.query.result.DistinctCountResult;

/**
 * @author jonathan
 */
public class MergeDistinctCountResults implements MiruResultMerger<DistinctCountResult> {

    @Override
    public DistinctCountResult merge(Optional<DistinctCountResult> last, DistinctCountResult current) {
        return current;

        /*TODO the merging is actually done by the filtering process, consider revisiting this
        if (!last.isPresent()) {
            return current;
        }

        ImmutableSet<MiruTermId> mergedAggregateTerms = ImmutableSet.<MiruTermId>builder()
            .addAll(last.get().aggregateTerms)
            .addAll(current.aggregateTerms)
            .build();
        int mergedCollectedDistincts = last.get().collectedDistincts + current.collectedDistincts;

        return new DistinctCountResult(mergedAggregateTerms, mergedCollectedDistincts);
        */
    }

    @Override
    public DistinctCountResult done(Optional<DistinctCountResult> last, DistinctCountResult alternative) {
        return last.or(alternative);
    }
}
