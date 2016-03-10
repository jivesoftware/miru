package com.jivesoftware.os.miru.catwalk.deployable;

import com.jivesoftware.os.miru.stream.plugins.catwalk.FeatureScore;
import java.util.List;

/**
 *
 * @author jonathan.colt
 */
class ModelFeatureScores {

    final boolean partitionIsClosed;
    final List<FeatureScore> featureScores;

    public ModelFeatureScores(boolean partitionIsClosed, List<FeatureScore> featureScores) {
        this.partitionIsClosed = partitionIsClosed;
        this.featureScores = featureScores;
    }

}
