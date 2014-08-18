package com.jivesoftware.os.miru.service.query.merge;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.jivesoftware.os.jive.utils.logger.MetricLogger;
import com.jivesoftware.os.jive.utils.logger.MetricLoggerFactory;
import com.jivesoftware.os.miru.api.base.MiruTermId;
import com.jivesoftware.os.miru.api.query.result.RecoResult;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;

/**
 *
 */
public class MergeRecoResults implements MiruResultMerger<RecoResult> {

    private static final MetricLogger log = MetricLoggerFactory.getLogger();

    private final int desiredNumberOfDistincts;

    public MergeRecoResults(int desiredNumberOfDistincts) {
        this.desiredNumberOfDistincts = desiredNumberOfDistincts;
    }

    /**
     * Merges the last and current results, returning the merged result.
     *
     * @param last the last merge result
     * @param currentResult the next result to merge
     * @return the merged result
     */
    @Override
    public RecoResult merge(Optional<RecoResult> last, RecoResult currentResult) {
        if (!last.isPresent()) {
            return currentResult;
        }

        RecoResult lastResult = last.get();

        Map<MiruTermId, RecoResult.Recommendation> ongoing = new HashMap<>();
        for (RecoResult.Recommendation recommendation : currentResult.results) {
            ongoing.put(recommendation.distinctValue, recommendation);
        }

        int size = currentResult.results.size() + last.get().results.size();

        List<RecoResult.Recommendation> mergedResults = Lists.newArrayListWithCapacity(size);
        for (RecoResult.Recommendation recommendation : lastResult.results) {
            RecoResult.Recommendation had = ongoing.remove(recommendation.distinctValue);
            if (had == null) {
                mergedResults.add(recommendation);
            } else {
                mergedResults.add(new RecoResult.Recommendation(recommendation.distinctValue, had.rank + recommendation.rank));
            }
        }
        for (RecoResult.Recommendation recommendation : currentResult.results) {
            if (ongoing.containsKey(recommendation.distinctValue)) {
                mergedResults.add(recommendation);
            }
        }

        RecoResult mergedResult = new RecoResult(ImmutableList.copyOf(mergedResults));

        logMergeResult(currentResult, lastResult, mergedResult);

        return mergedResult;
    }

    @Override
    public RecoResult done(Optional<RecoResult> last, RecoResult alternative) {
        return last.transform(new Function<RecoResult, RecoResult>() {
            @Nullable
            @Override
            public RecoResult apply(@Nullable RecoResult result) {
                List<RecoResult.Recommendation> results = Lists.newArrayList(result.results);
                //long t = System.currentTimeMillis();
                Collections.sort(results);
                //log.info("mergeReco: sorted in " + (System.currentTimeMillis() - t) + " ms");
                results = results.subList(0, Math.min(desiredNumberOfDistincts, results.size()));
                return new RecoResult(ImmutableList.copyOf(results));
            }
        }).or(alternative);
    }

    private void logMergeResult(RecoResult currentResult, RecoResult lastResult, RecoResult mergedResult) {
        log.debug("Merged:"
                + "\n  From: results={}"
                + "\n  With: results={}"
                + "\n  To:   results={}",
                lastResult.results.size(),
                currentResult.results.size(),
                mergedResult.results.size()
        );
    }
}
