package com.jivesoftware.os.miru.catwalk.deployable;

import com.jivesoftware.os.miru.catwalk.shared.FeatureScore;
import com.jivesoftware.os.miru.plugin.solution.MiruTimeRange;
import java.util.List;

/**
 * @author jonathan.colt
 */
public class ModelFeatureScores {

    public final boolean partitionIsClosed;
    public final long modelCount;
    public final long totalCount;
    public final List<FeatureScore> featureScores;
    public final MiruTimeRange timeRange;

    public ModelFeatureScores(boolean partitionIsClosed, long modelCount, long totalCount, List<FeatureScore> featureScores, MiruTimeRange timeRange) {
        this.partitionIsClosed = partitionIsClosed;
        this.modelCount = modelCount;
        this.totalCount = totalCount;
        this.featureScores = featureScores;
        this.timeRange = timeRange;
    }

}
