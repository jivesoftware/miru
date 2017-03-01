package com.jivesoftware.os.miru.catwalk.shared;

import com.jivesoftware.os.miru.api.base.MiruTermId;
import java.util.Map;

/**
 * @author jonathan.colt
 */
public class StrutModel {

    public final Map<StrutModelKey, StrutModelScore>[] model;
    public final long[] modelCounts;
    public final long totalCount;
    public final int[] numberOfModels;
    public final int[] totalNumPartitions;

    public StrutModel(Map<StrutModelKey, StrutModelScore>[] model,
        long[] modelCounts,
        long totalCount,
        int[] numberOfModels,
        int[] totalNumPartitions) {
        this.model = model;
        this.modelCounts = modelCounts;
        this.totalCount = totalCount;
        this.numberOfModels = numberOfModels;
        this.totalNumPartitions = totalNumPartitions;
    }

    public StrutModelScore score(int featureId, MiruTermId[] values) {
        return model[featureId].get(new StrutModelKey(values));
    }

}
