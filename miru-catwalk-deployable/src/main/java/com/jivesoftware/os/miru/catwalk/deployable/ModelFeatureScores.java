package com.jivesoftware.os.miru.catwalk.deployable;

import com.jivesoftware.os.miru.plugin.solution.MiruTimeRange;
import com.jivesoftware.os.miru.stream.plugins.catwalk.FeatureScore;
import java.util.List;

/**
 *
 * @author jonathan.colt
 */
class ModelFeatureScores {

    final boolean partitionIsClosed;
    final long[] modelCounts;
    final long totalCount;
    final List<FeatureScore> featureScores;
    final MiruTimeRange timeRange;

    public ModelFeatureScores(boolean partitionIsClosed, long[] modelCounts, long totalCount, List<FeatureScore> featureScores, MiruTimeRange timeRange) {
        this.partitionIsClosed = partitionIsClosed;
        this.modelCounts = modelCounts;
        this.totalCount = totalCount;
        this.featureScores = featureScores;
        this.timeRange = timeRange;
    }

}
